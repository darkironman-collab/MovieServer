package com.extreme.streamrelay;

import android.content.Context;
import android.content.SharedPreferences;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class RelayEngine {
    public static final int CHUNK_SIZE = 4 * 1024 * 1024;
    private final Context context;
    private final File cacheDir;
    private final SharedPreferences prefs;
    private final Map<Long, Future<?>> inFlight = new ConcurrentHashMap<>();
    private volatile String sourceUrl;
    private volatile long maxCacheBytes = 1024L * 1024L * 1024L;
    private volatile int threadCount = 8;
    private volatile long sourceLength = -1;
    private volatile String contentType = "application/octet-stream";
    private ExecutorService pool = Executors.newFixedThreadPool(8);
    private long reservedBytes = 0;

    public RelayEngine(Context context) {
        this.context = context.getApplicationContext();
        this.cacheDir = new File(context.getCacheDir(), "relay_chunks");
        //noinspection ResultOfMethodCallIgnored
        cacheDir.mkdirs();
        this.prefs = context.getSharedPreferences("relay", Context.MODE_PRIVATE);
    }

    public synchronized void configure(String url, long maxCacheBytes, int threads) throws Exception {
        boolean changed = sourceUrl == null || !sourceUrl.equals(url);
        sourceUrl = url;
        this.maxCacheBytes = Math.min(maxCacheBytes, 2048L * 1024L * 1024L);
        if (threads != threadCount) {
            pool.shutdownNow();
            threadCount = threads;
            pool = Executors.newFixedThreadPool(threadCount);
        }
        if (changed) clearCache();
        probe();
        updateCacheStats();
    }

    public String getSourceUrl() { return sourceUrl; }
    public long getSourceLength() { return sourceLength; }
    public String getContentType() { return contentType; }

    private void probe() throws Exception {
        HttpURLConnection c = openConnection(sourceUrl);
        c.setRequestMethod("HEAD");
        c.setConnectTimeout(12000);
        c.setReadTimeout(12000);
        int code = c.getResponseCode();
        if (code >= 200 && code < 400) {
            sourceLength = parseLong(c.getHeaderField("Content-Length"), -1);
            String ct = c.getContentType();
            if (ct != null) contentType = ct.split(";")[0];
        }
        c.disconnect();
        if (sourceLength < 0) {
            c = openConnection(sourceUrl);
            c.setRequestProperty("Range", "bytes=0-0");
            c.setConnectTimeout(12000);
            c.setReadTimeout(12000);
            code = c.getResponseCode();
            String cr = c.getHeaderField("Content-Range");
            if (cr != null && cr.contains("/")) sourceLength = parseLong(cr.substring(cr.lastIndexOf('/') + 1), -1);
            String ct = c.getContentType();
            if (ct != null) contentType = ct.split(";")[0];
            try (InputStream ignored = c.getInputStream()) { /* close */ }
            c.disconnect();
        }
    }

    private HttpURLConnection openConnection(String url) throws Exception {
        HttpURLConnection c = (HttpURLConnection) new URL(url).openConnection();
        c.setInstanceFollowRedirects(true);
        c.setRequestProperty("User-Agent", "Mozilla/5.0 (Android) ExtremeStreamRelay/0.1");
        c.setRequestProperty("Accept", "*/*");
        return c;
    }

    public File getChunk(long chunkIndex) throws Exception {
        if (sourceUrl == null) throw new IllegalStateException("No source URL configured");
        File f = chunkFile(chunkIndex);
        if (f.exists() && f.length() > 0) {
            //noinspection ResultOfMethodCallIgnored
            f.setLastModified(System.currentTimeMillis());
            schedulePrefetch(chunkIndex + 1);
            return f;
        }
        downloadChunkBlocking(chunkIndex);
        schedulePrefetch(chunkIndex + 1);
        return f;
    }

    private void schedulePrefetch(long first) {
        int ahead = Math.min(threadCount, 8);
        for (int i = 0; i < ahead; i++) {
            long idx = first + i;
            if (sourceLength > 0 && idx * (long) CHUNK_SIZE >= sourceLength) break;
            File f = chunkFile(idx);
            if (f.exists()) continue;
            inFlight.computeIfAbsent(idx, k -> pool.submit(() -> {
                try { downloadChunkBlocking(k); } catch (Exception ignored) {} finally { inFlight.remove(k); }
            }));
        }
    }

    private void downloadChunkBlocking(long chunkIndex) throws Exception {
        File finalFile = chunkFile(chunkIndex);
        if (finalFile.exists() && finalFile.length() > 0) return;
        synchronized (("chunk-" + chunkIndex).intern()) {
            if (finalFile.exists() && finalFile.length() > 0) return;
            long start = chunkIndex * (long) CHUNK_SIZE;
            long end = start + CHUNK_SIZE - 1;
            if (sourceLength > 0) end = Math.min(end, sourceLength - 1);
            if (sourceLength > 0 && start >= sourceLength) throw new IllegalArgumentException("Range past EOF");

            long expectedBytes = end - start + 1;
            reserveSpace(expectedBytes);
            HttpURLConnection c = null;
            File tmp = new File(cacheDir, chunkIndex + ".part");
            try {
                c = openConnection(sourceUrl);
                c.setConnectTimeout(15000);
                c.setReadTimeout(30000);
                c.setRequestProperty("Range", "bytes=" + start + "-" + end);
                int code = c.getResponseCode();
                if (code != 206 && !(code == 200 && start == 0)) {
                    throw new IllegalStateException("Upstream does not support requested byte range (HTTP " + code + ")");
                }

                try (InputStream in = c.getInputStream(); FileOutputStream out = new FileOutputStream(tmp)) {
                    byte[] buf = new byte[128 * 1024];
                    int n;
                    long remaining = expectedBytes;
                    while ((n = in.read(buf, 0, (int)Math.min(buf.length, remaining))) > 0) {
                        out.write(buf, 0, n);
                        remaining -= n;
                        if (remaining <= 0) break;
                    }
                }
                if (!tmp.renameTo(finalFile)) {
                    //noinspection ResultOfMethodCallIgnored
                    finalFile.delete();
                    if (!tmp.renameTo(finalFile)) throw new IllegalStateException("Cannot commit cache chunk");
                }
                //noinspection ResultOfMethodCallIgnored
                finalFile.setLastModified(System.currentTimeMillis());
            } finally {
                if (c != null) c.disconnect();
                if (tmp.exists()) { //noinspection ResultOfMethodCallIgnored
                    tmp.delete();
                }
                releaseSpace(expectedBytes);
                enforceCacheLimit();
                updateCacheStats();
            }
        }
    }

    private File chunkFile(long index) { return new File(cacheDir, index + ".chunk"); }

    private synchronized void reserveSpace(long bytes) {
        if (bytes > maxCacheBytes) throw new IllegalStateException("Chunk larger than cache limit");
        evictUntil(cacheSize() + reservedBytes + bytes <= maxCacheBytes, bytes);
        reservedBytes += bytes;
    }

    private synchronized void releaseSpace(long bytes) {
        reservedBytes = Math.max(0, reservedBytes - bytes);
        notifyAll();
    }

    private synchronized void enforceCacheLimit() {
        evictUntil(cacheSize() <= maxCacheBytes, 0);
    }

    private void evictUntil(boolean alreadyFits, long incomingBytes) {
        if (alreadyFits) return;
        List<File> files = new ArrayList<>();
        File[] all = cacheDir.listFiles((dir, name) -> name.endsWith(".chunk"));
        if (all == null) return;
        long total = 0;
        for (File f : all) { files.add(f); total += f.length(); }
        files.sort(Comparator.comparingLong(File::lastModified));
        for (File f : files) {
            if (total + reservedBytes + incomingBytes <= maxCacheBytes) break;
            long len = f.length();
            if (f.delete()) total -= len;
        }
        if (total + reservedBytes + incomingBytes > maxCacheBytes) {
            throw new IllegalStateException("Cache is busy; retry playback in a moment");
        }
    }

    public long cacheSize() {
        long total = 0;
        File[] all = cacheDir.listFiles((dir, name) -> name.endsWith(".chunk"));
        if (all != null) for (File f : all) total += f.length();
        return total;
    }

    private void updateCacheStats() {
        prefs.edit().putLong("cacheUsed", cacheSize()).putLong("cacheMax", maxCacheBytes).apply();
    }

    public synchronized void clearCache() {
        File[] files = cacheDir.listFiles();
        if (files != null) for (File f : files) //noinspection ResultOfMethodCallIgnored
            f.delete();
        updateCacheStats();
    }

    public void close() {
        pool.shutdownNow();
        inFlight.clear();
    }

    private static long parseLong(String s, long fallback) {
        try { return Long.parseLong(s); } catch (Exception e) { return fallback; }
    }
}
