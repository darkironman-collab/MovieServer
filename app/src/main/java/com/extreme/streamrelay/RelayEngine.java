package com.extreme.streamrelay;

import android.content.Context;
import android.content.SharedPreferences;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class RelayEngine {
    public static final int CHUNK_SIZE = 4 * 1024 * 1024;

    private final File cacheDir;
    private final SharedPreferences prefs;
    private final Map<Long, Future<?>> inFlight = new ConcurrentHashMap<>();

    private volatile String sourceUrl;
    private volatile long maxCacheBytes = 1024L * 1024L * 1024L;
    private volatile int threadCount = 8;
    private volatile long sourceLength = -1;
    private volatile String contentType = "application/octet-stream";
    private volatile String sourceExtension = "";
    private volatile String sourceFileName = "stream";
    private volatile boolean rangeSupported = false;

    private ExecutorService pool = Executors.newFixedThreadPool(8);
    private long reservedBytes = 0;

    public RelayEngine(Context context) {
        Context app = context.getApplicationContext();
        this.cacheDir = new File(app.getCacheDir(), "relay_chunks");
        //noinspection ResultOfMethodCallIgnored
        cacheDir.mkdirs();
        this.prefs = app.getSharedPreferences("relay", Context.MODE_PRIVATE);
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
        sourceLength = -1;
        contentType = "application/octet-stream";
        rangeSupported = false;
        sourceFileName = inferFileName(url);
        sourceExtension = inferExtensionFromName(sourceFileName);

        probeWithRangeGet();

        if (!rangeSupported && sourceLength > CHUNK_SIZE) {
            throw new IllegalStateException("Source server does not support HTTP byte ranges. Multi-thread relay requires Range support.");
        }

        if (sourceExtension.isEmpty()) {
            sourceExtension = extensionFromContentType(contentType);
        }

        String streamPath = getStreamPath();
        prefs.edit()
                .putString("contentType", contentType)
                .putString("streamPath", streamPath)
                .putString("sourceFileName", sourceFileName)
                .putLong("sourceLength", sourceLength)
                .putBoolean("rangeSupported", rangeSupported)
                .apply();
        updateCacheStats();
    }

    public String getSourceUrl() { return sourceUrl; }
    public long getSourceLength() { return sourceLength; }
    public String getContentType() { return contentType; }
    public boolean isRangeSupported() { return rangeSupported; }
    public String getSourceFileName() { return sourceFileName; }

    public String getStreamPath() {
        if (sourceExtension == null || sourceExtension.isEmpty()) return "/stream";
        return "/stream." + sourceExtension;
    }

    private void probeWithRangeGet() throws Exception {
        HttpURLConnection c = null;
        try {
            c = openConnection(sourceUrl);
            c.setConnectTimeout(15000);
            c.setReadTimeout(15000);
            c.setRequestProperty("Range", "bytes=0-0");

            int code = c.getResponseCode();
            captureResponseMetadata(c);

            if (code == 206) {
                rangeSupported = true;
                String cr = c.getHeaderField("Content-Range");
                if (cr != null && cr.contains("/")) {
                    String total = cr.substring(cr.lastIndexOf('/') + 1).trim();
                    if (!"*".equals(total)) sourceLength = parseLong(total, -1);
                }
                try (InputStream in = c.getInputStream()) {
                    in.read();
                }
                return;
            }

            if (code >= 200 && code < 300) {
                rangeSupported = false;
                sourceLength = parseLong(c.getHeaderField("Content-Length"), sourceLength);
                try (InputStream in = c.getInputStream()) {
                    byte[] one = new byte[1];
                    in.read(one);
                }
                return;
            }

            throw new IllegalStateException("Upstream HTTP " + code + errorSuffix(c));
        } finally {
            if (c != null) c.disconnect();
        }
    }

    private void captureResponseMetadata(HttpURLConnection c) {
        String ct = c.getContentType();
        if (ct != null && !ct.trim().isEmpty()) {
            contentType = ct.split(";", 2)[0].trim().toLowerCase(Locale.ROOT);
        }
        long length = parseLong(c.getHeaderField("Content-Length"), -1);
        if (sourceLength < 0 && length >= 0) sourceLength = length;
    }

    private HttpURLConnection openConnection(String url) throws Exception {
        HttpURLConnection c = (HttpURLConnection) new URL(url).openConnection();
        c.setInstanceFollowRedirects(true);
        c.setRequestProperty("User-Agent", "Mozilla/5.0 (Linux; Android 16) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0 Mobile Safari/537.36");
        c.setRequestProperty("Accept", "*/*");
        c.setRequestProperty("Accept-Encoding", "identity");
        c.setRequestProperty("Connection", "keep-alive");
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
        if (!rangeSupported) return;
        int ahead = Math.min(threadCount, 16);
        for (int i = 0; i < ahead; i++) {
            long idx = first + i;
            if (sourceLength > 0 && idx * (long) CHUNK_SIZE >= sourceLength) break;
            File f = chunkFile(idx);
            if (f.exists()) continue;
            inFlight.computeIfAbsent(idx, k -> pool.submit(() -> {
                try {
                    downloadChunkBlocking(k);
                } catch (Exception ignored) {
                } finally {
                    inFlight.remove(k);
                }
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
            if (sourceLength > 0 && start >= sourceLength) throw new IllegalArgumentException("Requested range is past end of file");

            long expectedBytes = end - start + 1;
            reserveSpace(expectedBytes);
            HttpURLConnection c = null;
            File tmp = new File(cacheDir, chunkIndex + ".part");

            try {
                c = openConnection(sourceUrl);
                c.setConnectTimeout(15000);
                c.setReadTimeout(45000);
                if (rangeSupported) c.setRequestProperty("Range", "bytes=" + start + "-" + end);

                int code = c.getResponseCode();
                captureResponseMetadata(c);

                if (rangeSupported && code != 206) {
                    throw new IllegalStateException("Upstream stopped honoring byte ranges (HTTP " + code + ")");
                }
                if (!rangeSupported && !(code >= 200 && code < 300 && start == 0)) {
                    throw new IllegalStateException("Upstream cannot provide requested byte range (HTTP " + code + ")");
                }

                try (InputStream in = c.getInputStream(); FileOutputStream out = new FileOutputStream(tmp)) {
                    byte[] buf = new byte[128 * 1024];
                    long remaining = expectedBytes;
                    while (remaining > 0) {
                        int n = in.read(buf, 0, (int) Math.min(buf.length, remaining));
                        if (n < 0) break;
                        out.write(buf, 0, n);
                        remaining -= n;
                    }
                }

                if (tmp.length() <= 0) throw new IllegalStateException("Upstream returned no media data");
                if (!tmp.renameTo(finalFile)) {
                    //noinspection ResultOfMethodCallIgnored
                    finalFile.delete();
                    if (!tmp.renameTo(finalFile)) throw new IllegalStateException("Cannot commit cache chunk");
                }
                //noinspection ResultOfMethodCallIgnored
                finalFile.setLastModified(System.currentTimeMillis());
            } finally {
                if (c != null) c.disconnect();
                if (tmp.exists()) {
                    //noinspection ResultOfMethodCallIgnored
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
        for (File f : all) {
            files.add(f);
            total += f.length();
        }
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
        if (files != null) {
            for (File f : files) {
                //noinspection ResultOfMethodCallIgnored
                f.delete();
            }
        }
        updateCacheStats();
    }

    public void close() {
        pool.shutdownNow();
        inFlight.clear();
    }

    private String inferFileName(String rawUrl) {
        try {
            String path = new URL(rawUrl).getPath();
            if (path != null && !path.isEmpty()) {
                int slash = path.lastIndexOf('/');
                String name = slash >= 0 ? path.substring(slash + 1) : path;
                if (!name.isEmpty()) return sanitizeFileName(name);
            }
        } catch (Exception ignored) {
        }
        return "stream";
    }

    private String sanitizeFileName(String name) {
        String clean = name.replaceAll("[^A-Za-z0-9._-]", "_");
        return clean.isEmpty() ? "stream" : clean;
    }

    private String inferExtensionFromName(String name) {
        if (name == null) return "";
        int dot = name.lastIndexOf('.');
        if (dot < 0 || dot == name.length() - 1) return "";
        String ext = name.substring(dot + 1).toLowerCase(Locale.ROOT);
        switch (ext) {
            case "mkv": case "mp4": case "m4v": case "webm": case "avi":
            case "mov": case "ts": case "m2ts": case "mpg": case "mpeg":
                return ext;
            default:
                return "";
        }
    }

    private String extensionFromContentType(String type) {
        if (type == null) return "";
        String t = type.toLowerCase(Locale.ROOT);
        if (t.contains("matroska")) return "mkv";
        if (t.contains("video/mp4") || t.contains("application/mp4")) return "mp4";
        if (t.contains("webm")) return "webm";
        if (t.contains("quicktime")) return "mov";
        if (t.contains("mp2t") || t.contains("mpegts")) return "ts";
        if (t.contains("x-msvideo")) return "avi";
        return "";
    }

    private String errorSuffix(HttpURLConnection c) {
        try {
            InputStream err = c.getErrorStream();
            if (err == null) return "";
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            byte[] buf = new byte[256];
            int total = 0;
            int n;
            while (total < 512 && (n = err.read(buf, 0, Math.min(buf.length, 512 - total))) > 0) {
                out.write(buf, 0, n);
                total += n;
            }
            String text = out.toString("UTF-8").replaceAll("\\s+", " ").trim();
            if (text.isEmpty()) return "";
            return ": " + text;
        } catch (Exception ignored) {
            return "";
        }
    }

    private static long parseLong(String s, long fallback) {
        try {
            return Long.parseLong(s);
        } catch (Exception e) {
            return fallback;
        }
    }
}
