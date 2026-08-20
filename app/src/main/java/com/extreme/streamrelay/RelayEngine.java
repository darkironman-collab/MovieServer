package com.extreme.streamrelay;

import android.content.Context;
import android.content.SharedPreferences;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
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
    private static final int PROBE_BYTES = 4096;
    private static final long HARD_CACHE_MAX = 2048L * 1024L * 1024L;

    private final File cacheDir;
    private final SharedPreferences prefs;
    private final Map<Long, Future<?>> inFlight = new ConcurrentHashMap<>();
    private final Object sequentialLock = new Object();

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

    // Used only when the origin ignores Range requests. One persistent upstream
    // connection is consumed from byte 0 onward while the local server still
    // exposes normal byte-addressed chunks to the player.
    private HttpURLConnection sequentialConnection;
    private InputStream sequentialInput;
    private long sequentialNextChunkIndex = 0;
    private boolean sequentialEof = false;

    public RelayEngine(Context context) {
        Context app = context.getApplicationContext();
        this.cacheDir = new File(app.getCacheDir(), "relay_chunks");
        //noinspection ResultOfMethodCallIgnored
        cacheDir.mkdirs();
        this.prefs = app.getSharedPreferences("relay", Context.MODE_PRIVATE);
    }

    public synchronized void configure(String url, long requestedCacheBytes, int threads) throws Exception {
        boolean changed = sourceUrl == null || !sourceUrl.equals(url);
        sourceUrl = url;
        maxCacheBytes = Math.max(CHUNK_SIZE, Math.min(requestedCacheBytes, HARD_CACHE_MAX));

        if (threads != threadCount) {
            pool.shutdownNow();
            threadCount = Math.max(2, Math.min(threads, 16));
            pool = Executors.newFixedThreadPool(threadCount);
            inFlight.clear();
        }

        closeSequentialSession();
        if (changed) clearCache();

        sourceLength = -1;
        contentType = "application/octet-stream";
        rangeSupported = false;
        sourceFileName = inferFileName(url);
        sourceExtension = inferExtensionFromName(sourceFileName);

        probeWithRangeGet();

        String streamPath = getStreamPath();
        prefs.edit()
                .putString("contentType", contentType)
                .putString("streamPath", streamPath)
                .putString("sourceFileName", sourceFileName)
                .putLong("sourceLength", sourceLength)
                .putBoolean("rangeSupported", rangeSupported)
                .putString("relayMode", rangeSupported ? "multi-thread range" : "sequential fallback")
                .putLong("cacheMax", maxCacheBytes)
                .apply();
        updateCacheStats();
    }

    public String getSourceUrl() { return sourceUrl; }
    public long getSourceLength() { return sourceLength; }
    public String getContentType() { return contentType; }
    public boolean isRangeSupported() { return rangeSupported; }
    public String getSourceFileName() { return sourceFileName; }
    public long getMaxCacheBytes() { return maxCacheBytes; }
    public String getRelayMode() { return rangeSupported ? "multi-thread range" : "sequential fallback"; }

    public String getStreamPath() {
        if (sourceExtension == null || sourceExtension.isEmpty()) return "/stream";
        return "/stream." + sourceExtension;
    }

    private void probeWithRangeGet() throws Exception {
        HttpURLConnection c = null;
        byte[] prefix = new byte[0];
        try {
            c = openConnection(sourceUrl);
            c.setConnectTimeout(15000);
            c.setReadTimeout(15000);
            c.setRequestProperty("Range", "bytes=0-" + (PROBE_BYTES - 1));

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
                    prefix = readPrefix(in, PROBE_BYTES);
                }
            } else if (code >= 200 && code < 300) {
                // Origin ignored Range. This is now supported by the sequential
                // fallback; do not reject large files.
                rangeSupported = false;
                sourceLength = parseLong(c.getHeaderField("Content-Length"), sourceLength);
                try (InputStream in = c.getInputStream()) {
                    prefix = readPrefix(in, PROBE_BYTES);
                }
            } else {
                throw new IllegalStateException("Upstream HTTP " + code + errorSuffix(c));
            }
        } finally {
            if (c != null) c.disconnect();
        }

        if (sourceExtension.isEmpty()) sourceExtension = extensionFromContentType(contentType);
        if (sourceExtension.isEmpty()) sourceExtension = extensionFromMagic(prefix);

        if (!sourceExtension.isEmpty()) {
            if (inferExtensionFromName(sourceFileName).isEmpty()) {
                sourceFileName = sourceFileName + "." + sourceExtension;
            }
            if ("application/octet-stream".equals(contentType) || contentType.isEmpty()) {
                contentType = contentTypeFromExtension(sourceExtension);
            }
        }
    }

    private byte[] readPrefix(InputStream in, int maxBytes) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream(maxBytes);
        byte[] buf = new byte[1024];
        int remaining = maxBytes;
        while (remaining > 0) {
            int n = in.read(buf, 0, Math.min(buf.length, remaining));
            if (n < 0) break;
            out.write(buf, 0, n);
            remaining -= n;
        }
        return out.toByteArray();
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
        if (chunkIndex < 0) throw new IllegalArgumentException("Negative chunk index");

        File f = chunkFile(chunkIndex);
        if (f.exists() && f.length() > 0) {
            //noinspection ResultOfMethodCallIgnored
            f.setLastModified(System.currentTimeMillis());
            schedulePrefetch(chunkIndex + 1);
            return f;
        }

        if (rangeSupported) {
            downloadRangeChunkBlocking(chunkIndex);
        } else {
            downloadSequentialThrough(chunkIndex);
        }

        f = chunkFile(chunkIndex);
        if (!f.exists() || f.length() <= 0) {
            throw new IllegalStateException("Unable to cache requested media chunk " + chunkIndex);
        }
        //noinspection ResultOfMethodCallIgnored
        f.setLastModified(System.currentTimeMillis());
        schedulePrefetch(chunkIndex + 1);
        return f;
    }

    private void schedulePrefetch(long first) {
        if (sourceLength > 0 && first * (long) CHUNK_SIZE >= sourceLength) return;

        if (!rangeSupported) {
            // No true parallelism is possible if the origin ignores Range.
            // Still keep one chunk warm ahead of the player using the same
            // sequential connection.
            File next = chunkFile(first);
            if (next.exists()) return;
            inFlight.computeIfAbsent(first, k -> pool.submit(() -> {
                try {
                    downloadSequentialThrough(k);
                } catch (Exception ignored) {
                } finally {
                    inFlight.remove(k);
                }
            }));
            return;
        }

        int ahead = Math.min(threadCount, 16);
        for (int i = 0; i < ahead; i++) {
            long idx = first + i;
            if (sourceLength > 0 && idx * (long) CHUNK_SIZE >= sourceLength) break;
            File f = chunkFile(idx);
            if (f.exists()) continue;
            inFlight.computeIfAbsent(idx, k -> pool.submit(() -> {
                try {
                    downloadRangeChunkBlocking(k);
                } catch (Exception ignored) {
                } finally {
                    inFlight.remove(k);
                }
            }));
        }
    }

    private void downloadRangeChunkBlocking(long chunkIndex) throws Exception {
        File finalFile = chunkFile(chunkIndex);
        if (finalFile.exists() && finalFile.length() > 0) return;

        synchronized (("range-chunk-" + chunkIndex).intern()) {
            if (finalFile.exists() && finalFile.length() > 0) return;

            long start = chunkIndex * (long) CHUNK_SIZE;
            long end = start + CHUNK_SIZE - 1;
            if (sourceLength > 0) end = Math.min(end, sourceLength - 1);
            if (sourceLength > 0 && start >= sourceLength) {
                throw new IllegalArgumentException("Requested range is past end of file");
            }

            long expectedBytes = end - start + 1;
            reserveSpace(expectedBytes);
            HttpURLConnection c = null;
            File tmp = new File(cacheDir, chunkIndex + ".part");

            try {
                c = openConnection(sourceUrl);
                c.setConnectTimeout(15000);
                c.setReadTimeout(45000);
                c.setRequestProperty("Range", "bytes=" + start + "-" + end);

                int code = c.getResponseCode();
                captureResponseMetadata(c);
                if (code != 206) {
                    throw new IllegalStateException("Upstream stopped honoring byte ranges (HTTP " + code + ")");
                }

                long written;
                try (InputStream in = c.getInputStream(); FileOutputStream out = new FileOutputStream(tmp)) {
                    written = copyAtMost(in, out, expectedBytes);
                }

                if (written <= 0) throw new IllegalStateException("Upstream returned no media data");
                commitTemp(tmp, finalFile);
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

    private void downloadSequentialThrough(long targetChunkIndex) throws Exception {
        File target = chunkFile(targetChunkIndex);
        if (target.exists() && target.length() > 0) return;

        synchronized (sequentialLock) {
            target = chunkFile(targetChunkIndex);
            if (target.exists() && target.length() > 0) return;

            // If the player asks for an older chunk that has already been
            // evicted from the rolling cache, restart the non-range source at
            // byte 0 and advance sequentially. This is slower than true Range
            // seeking, but it keeps playback compatible with non-range hosts.
            if (targetChunkIndex < sequentialNextChunkIndex) {
                closeSequentialSessionLocked();
            }

            ensureSequentialSessionLocked();

            while (sequentialNextChunkIndex <= targetChunkIndex) {
                long idx = sequentialNextChunkIndex;
                long start = idx * (long) CHUNK_SIZE;

                if (sourceLength > 0 && start >= sourceLength) {
                    sequentialEof = true;
                    closeSequentialSessionLocked();
                    break;
                }

                long expectedBytes = sourceLength > 0
                        ? Math.min((long) CHUNK_SIZE, sourceLength - start)
                        : CHUNK_SIZE;

                reserveSpace(expectedBytes);
                File tmp = new File(cacheDir, idx + ".part");
                File finalFile = chunkFile(idx);
                long written = 0;

                try {
                    if (finalFile.exists() && finalFile.length() > 0) {
                        // The upstream stream still has to advance over these
                        // bytes to keep its position aligned.
                        written = discardAtMost(sequentialInput, expectedBytes);
                    } else {
                        try (FileOutputStream out = new FileOutputStream(tmp)) {
                            written = copyAtMost(sequentialInput, out, expectedBytes);
                        }
                        if (written > 0) commitTemp(tmp, finalFile);
                    }

                    if (written <= 0) {
                        if (sourceLength < 0 || start < sourceLength) {
                            sourceLength = start;
                            prefs.edit().putLong("sourceLength", sourceLength).apply();
                        }
                        sequentialEof = true;
                        closeSequentialSessionLocked();
                        break;
                    }

                    sequentialNextChunkIndex++;

                    if (written < expectedBytes) {
                        sourceLength = start + written;
                        prefs.edit().putLong("sourceLength", sourceLength).apply();
                        sequentialEof = true;
                        closeSequentialSessionLocked();
                        break;
                    }
                } finally {
                    if (tmp.exists()) {
                        //noinspection ResultOfMethodCallIgnored
                        tmp.delete();
                    }
                    releaseSpace(expectedBytes);
                    enforceCacheLimit();
                    updateCacheStats();
                }
            }

            target = chunkFile(targetChunkIndex);
            if (!target.exists() || target.length() <= 0) {
                if (sequentialEof) throw new IllegalArgumentException("Requested position is past end of source");
                throw new IllegalStateException("Sequential source could not produce requested chunk");
            }
        }
    }

    private void ensureSequentialSessionLocked() throws Exception {
        if (sequentialInput != null) return;

        sequentialConnection = openConnection(sourceUrl);
        sequentialConnection.setConnectTimeout(15000);
        sequentialConnection.setReadTimeout(60000);
        int code = sequentialConnection.getResponseCode();
        captureResponseMetadata(sequentialConnection);
        if (code < 200 || code >= 300) {
            String detail = "Upstream HTTP " + code + errorSuffix(sequentialConnection);
            closeSequentialSessionLocked();
            throw new IllegalStateException(detail);
        }

        sequentialInput = sequentialConnection.getInputStream();
        sequentialNextChunkIndex = 0;
        sequentialEof = false;
    }

    private void closeSequentialSession() {
        synchronized (sequentialLock) {
            closeSequentialSessionLocked();
        }
    }

    private void closeSequentialSessionLocked() {
        try {
            if (sequentialInput != null) sequentialInput.close();
        } catch (Exception ignored) {
        }
        try {
            if (sequentialConnection != null) sequentialConnection.disconnect();
        } catch (Exception ignored) {
        }
        sequentialInput = null;
        sequentialConnection = null;
        sequentialNextChunkIndex = 0;
        sequentialEof = false;
    }

    private long copyAtMost(InputStream in, FileOutputStream out, long maxBytes) throws Exception {
        byte[] buf = new byte[128 * 1024];
        long remaining = maxBytes;
        long written = 0;
        while (remaining > 0) {
            int n = in.read(buf, 0, (int) Math.min(buf.length, remaining));
            if (n < 0) break;
            out.write(buf, 0, n);
            remaining -= n;
            written += n;
        }
        return written;
    }

    private long discardAtMost(InputStream in, long maxBytes) throws Exception {
        byte[] buf = new byte[128 * 1024];
        long remaining = maxBytes;
        long consumed = 0;
        while (remaining > 0) {
            int n = in.read(buf, 0, (int) Math.min(buf.length, remaining));
            if (n < 0) break;
            remaining -= n;
            consumed += n;
        }
        return consumed;
    }

    private void commitTemp(File tmp, File finalFile) throws Exception {
        if (tmp.length() <= 0) throw new IllegalStateException("Upstream returned no media data");
        //noinspection ResultOfMethodCallIgnored
        finalFile.delete();
        if (!tmp.renameTo(finalFile)) throw new IllegalStateException("Cannot commit cache chunk");
        //noinspection ResultOfMethodCallIgnored
        finalFile.setLastModified(System.currentTimeMillis());
    }

    private File chunkFile(long index) {
        return new File(cacheDir, index + ".chunk");
    }

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
        if (all != null) {
            for (File f : all) total += f.length();
        }
        return total;
    }

    private void updateCacheStats() {
        prefs.edit()
                .putLong("cacheUsed", cacheSize())
                .putLong("cacheMax", maxCacheBytes)
                .apply();
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
        closeSequentialSession();
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

    private String extensionFromMagic(byte[] b) {
        if (b == null || b.length < 4) return "";

        if ((b[0] & 0xff) == 0x1A && (b[1] & 0xff) == 0x45 &&
                (b[2] & 0xff) == 0xDF && (b[3] & 0xff) == 0xA3) {
            String text = new String(b, StandardCharsets.ISO_8859_1).toLowerCase(Locale.ROOT);
            return text.contains("webm") ? "webm" : "mkv";
        }

        if (b.length >= 12 && asciiEquals(b, 4, "ftyp")) return "mp4";
        if (b.length >= 12 && asciiEquals(b, 0, "RIFF") && asciiEquals(b, 8, "AVI ")) return "avi";
        if (b.length >= 8 && asciiEquals(b, 4, "moov")) return "mov";
        if (b.length >= 3 && (b[0] & 0xff) == 0x00 && (b[1] & 0xff) == 0x00 && (b[2] & 0xff) == 0x01) return "mpeg";
        if (b.length >= 377 && (b[0] & 0xff) == 0x47 && (b[188] & 0xff) == 0x47 && (b[376] & 0xff) == 0x47) return "ts";
        return "";
    }

    private boolean asciiEquals(byte[] b, int offset, String s) {
        if (offset < 0 || offset + s.length() > b.length) return false;
        for (int i = 0; i < s.length(); i++) {
            if ((byte) s.charAt(i) != b[offset + i]) return false;
        }
        return true;
    }

    private String contentTypeFromExtension(String ext) {
        if (ext == null) return "application/octet-stream";
        switch (ext.toLowerCase(Locale.ROOT)) {
            case "mkv": return "video/x-matroska";
            case "mp4": case "m4v": return "video/mp4";
            case "webm": return "video/webm";
            case "avi": return "video/x-msvideo";
            case "mov": return "video/quicktime";
            case "ts": case "m2ts": return "video/mp2t";
            case "mpg": case "mpeg": return "video/mpeg";
            default: return "application/octet-stream";
        }
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
            return text.isEmpty() ? "" : ": " + text;
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
