package com.extreme.streamrelay;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RelayHttpServer {
    public static final int PORT = 8787;

    private final RelayEngine engine;
    private final ExecutorService clients = Executors.newCachedThreadPool();
    private volatile boolean running;
    private ServerSocket serverSocket;
    private Thread acceptThread;

    public RelayHttpServer(RelayEngine engine) {
        this.engine = engine;
    }

    public synchronized void start() throws Exception {
        if (running) return;
        serverSocket = new ServerSocket(PORT);
        serverSocket.setReuseAddress(true);
        running = true;
        acceptThread = new Thread(this::acceptLoop, "relay-http-accept");
        acceptThread.start();
    }

    public synchronized void stop() {
        running = false;
        try {
            if (serverSocket != null) serverSocket.close();
        } catch (Exception ignored) {
        }
        clients.shutdownNow();
    }

    private void acceptLoop() {
        while (running) {
            try {
                Socket s = serverSocket.accept();
                s.setTcpNoDelay(true);
                clients.submit(() -> handle(s));
            } catch (Exception e) {
                if (running) e.printStackTrace();
            }
        }
    }

    private void handle(Socket socket) {
        try (Socket s = socket;
             InputStream rawIn = new BufferedInputStream(s.getInputStream());
             OutputStream out = new BufferedOutputStream(s.getOutputStream())) {

            String requestLine = readLine(rawIn);
            if (requestLine == null || requestLine.isEmpty()) return;
            String[] parts = requestLine.split(" ");
            if (parts.length < 2) return;

            String method = parts[0].toUpperCase(Locale.ROOT);
            String path = parts[1].split("\\?", 2)[0];
            Map<String, String> headers = new HashMap<>();
            String line;
            while ((line = readLine(rawIn)) != null && !line.isEmpty()) {
                int idx = line.indexOf(':');
                if (idx > 0) {
                    headers.put(
                            line.substring(0, idx).trim().toLowerCase(Locale.ROOT),
                            line.substring(idx + 1).trim()
                    );
                }
            }

            if ("/health".equals(path)) {
                sendText(out, 200, "OK", "text/plain", "ok\n");
                return;
            }

            if ("/permanent.m3u".equals(path) || "/playlist.m3u".equals(path)) {
                sendText(out, 410, "Gone", "text/plain", "M3U output removed. Use the direct stream URL shown in the app.\n");
                return;
            }

            if (!isMediaPath(path)) {
                sendText(out, 404, "Not Found", "text/plain", "Not found\n");
                return;
            }

            if (engine.getSourceUrl() == null) {
                sendText(out, 503, "Service Unavailable", "text/plain", "No media URL configured\n");
                return;
            }

            if (!"GET".equals(method) && !"HEAD".equals(method)) {
                sendText(out, 405, "Method Not Allowed", "text/plain", "GET/HEAD only\n");
                return;
            }

            if (engine.isRangeSupported()) {
                serveRangeCapableMedia(out, headers.get("range"), "HEAD".equals(method));
            } else {
                // IMPORTANT: the upstream origin itself cannot seek. Do not lie to
                // the PC player by advertising Range support. Give the player the
                // complete file size, but always deliver a progressive 200 response
                // from byte zero. This prevents player tail-probes from forcing the
                // phone to download gigabytes before playback can start.
                serveProgressiveMedia(out, "HEAD".equals(method));
            }
        } catch (Exception ignored) {
            // Player disconnects/reopens sockets during probing and startup.
        }
    }

    private boolean isMediaPath(String path) {
        if (path == null) return false;
        if ("/play".equals(path) || "/stream".equals(path)) return true;
        return path.startsWith("/stream.");
    }

    private void serveProgressiveMedia(OutputStream out, boolean headOnly) throws Exception {
        long length = engine.getSourceLength();

        StringBuilder h = new StringBuilder();
        h.append("HTTP/1.1 200 OK\r\n");
        h.append("Content-Type: ").append(engine.getContentType()).append("\r\n");
        h.append("Accept-Ranges: none\r\n");
        h.append("Cache-Control: no-store\r\n");
        h.append("Content-Disposition: inline; filename=\"")
                .append(safeHeaderFileName(engine.getSourceFileName()))
                .append("\"\r\n");
        if (length > 0) h.append("Content-Length: ").append(length).append("\r\n");
        h.append("Connection: close\r\n\r\n");

        out.write(h.toString().getBytes(StandardCharsets.US_ASCII));
        if (headOnly) {
            out.flush();
            return;
        }

        // Always start at byte 0 in non-range mode. The phone reads the Internet
        // source sequentially and serves it over LAN while the rolling cache stays
        // capped by the user's selected cache size (hard max 2 GB).
        long pos = 0;
        while (length <= 0 || pos < length) {
            long chunkIndex = pos / RelayEngine.CHUNK_SIZE;
            File chunk = engine.getChunk(chunkIndex);
            long available = chunk.length();
            if (available <= 0) break;

            try (FileInputStream fis = new FileInputStream(chunk)) {
                byte[] buf = new byte[128 * 1024];
                int n;
                while ((n = fis.read(buf)) >= 0) {
                    if (n == 0) continue;
                    if (length > 0 && pos + n > length) {
                        n = (int) (length - pos);
                        if (n <= 0) break;
                    }
                    out.write(buf, 0, n);
                    pos += n;
                    if (length > 0 && pos >= length) break;
                }
                out.flush();
            }

            if (available < RelayEngine.CHUNK_SIZE) break;
        }
    }

    private void serveRangeCapableMedia(OutputStream out, String range, boolean headOnly) throws Exception {
        long length = engine.getSourceLength();
        long start = 0;
        long end = length > 0 ? length - 1 : Long.MAX_VALUE;
        boolean partial = false;

        if (range != null && range.toLowerCase(Locale.ROOT).startsWith("bytes=")) {
            String spec = range.substring(6).split(",", 2)[0].trim();
            String[] se = spec.split("-", 2);

            if (se[0].isEmpty()) {
                if (length <= 0) {
                    sendText(out, 416, "Range Not Satisfiable", "text/plain", "Suffix ranges require a known source size\n");
                    return;
                }
                long suffix = Long.parseLong(se.length > 1 ? se[1] : "0");
                suffix = Math.min(suffix, length);
                start = Math.max(0, length - suffix);
                end = length - 1;
            } else {
                start = Long.parseLong(se[0]);
                if (se.length > 1 && !se[1].isEmpty()) end = Long.parseLong(se[1]);
            }
            partial = true;
        }

        if (length > 0) end = Math.min(end, length - 1);
        if (length > 0 && start >= length) {
            String h = "HTTP/1.1 416 Range Not Satisfiable\r\n" +
                    "Content-Range: bytes */" + length + "\r\n" +
                    "Connection: close\r\n\r\n";
            out.write(h.getBytes(StandardCharsets.US_ASCII));
            out.flush();
            return;
        }

        long responseLength = (length > 0 || end != Long.MAX_VALUE) ? (end - start + 1) : -1;
        StringBuilder h = new StringBuilder();
        h.append(partial ? "HTTP/1.1 206 Partial Content\r\n" : "HTTP/1.1 200 OK\r\n");
        h.append("Content-Type: ").append(engine.getContentType()).append("\r\n");
        h.append("Accept-Ranges: bytes\r\n");
        h.append("Cache-Control: no-store\r\n");
        h.append("Content-Disposition: inline; filename=\"")
                .append(safeHeaderFileName(engine.getSourceFileName()))
                .append("\"\r\n");

        if (partial && length > 0) {
            h.append("Content-Range: bytes ")
                    .append(start).append('-').append(end).append('/').append(length)
                    .append("\r\n");
        }
        if (responseLength >= 0) h.append("Content-Length: ").append(responseLength).append("\r\n");
        h.append("Connection: close\r\n\r\n");

        out.write(h.toString().getBytes(StandardCharsets.US_ASCII));
        if (headOnly) {
            out.flush();
            return;
        }

        long pos = start;
        while (pos <= end) {
            long chunkIndex = pos / RelayEngine.CHUNK_SIZE;
            int offset = (int) (pos % RelayEngine.CHUNK_SIZE);
            File chunk = engine.getChunk(chunkIndex);
            long available = chunk.length() - offset;
            if (available <= 0) break;

            long wanted = end == Long.MAX_VALUE
                    ? available
                    : Math.min(available, end - pos + 1);

            try (FileInputStream fis = new FileInputStream(chunk)) {
                long skipped = 0;
                while (skipped < offset) {
                    long n = fis.skip(offset - skipped);
                    if (n <= 0) break;
                    skipped += n;
                }

                byte[] buf = new byte[128 * 1024];
                long remain = wanted;
                while (remain > 0) {
                    int n = fis.read(buf, 0, (int) Math.min(buf.length, remain));
                    if (n < 0) break;
                    out.write(buf, 0, n);
                    remain -= n;
                    pos += n;
                }
                out.flush();
                if (remain > 0) break;
            }

            if (length > 0 && pos >= length) break;
        }
    }

    private String safeHeaderFileName(String name) {
        if (name == null || name.isEmpty()) return "stream";
        return name.replace("\\", "_").replace("\"", "_").replace("\r", "_").replace("\n", "_");
    }

    private void sendText(OutputStream out, int code, String reason, String type, String body) throws Exception {
        byte[] b = body.getBytes(StandardCharsets.UTF_8);
        String h = "HTTP/1.1 " + code + " " + reason + "\r\n" +
                "Content-Type: " + type + "\r\n" +
                "Content-Length: " + b.length + "\r\n" +
                "Connection: close\r\n\r\n";
        out.write(h.getBytes(StandardCharsets.US_ASCII));
        out.write(b);
        out.flush();
    }

    private String readLine(InputStream in) throws Exception {
        StringBuilder sb = new StringBuilder();
        int prev = -1;
        int cur;
        while ((cur = in.read()) != -1) {
            if (prev == '\r' && cur == '\n') {
                sb.setLength(Math.max(0, sb.length() - 1));
                return sb.toString();
            }
            sb.append((char) cur);
            prev = cur;
            if (sb.length() > 8192) throw new IllegalArgumentException("Header line too long");
        }
        return sb.length() == 0 ? null : sb.toString();
    }
}
