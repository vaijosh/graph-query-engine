package com.graphqueryengine.http;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

/**
 * Thin wrapper around the JDK built-in {@link HttpServer} that provides a
 * fluent route-registration API ({@code get}, {@code post}, {@code delete})
 * and a Javalin-compatible {@link #port()} / {@link #stop()} contract.
 *
 * <p>Using the JDK server means <em>zero external HTTP-server dependencies</em>
 * and therefore no Jetty CVEs.
 */
public class RouterServer {

    private final HttpServer server;

    /** All routes are dispatched through a single "/" context. */
    private final List<Route> routes = new ArrayList<>();

    private RouterServer(HttpServer server) {
        this.server = server;
        server.createContext("/", this::dispatch);
        server.setExecutor(Executors.newCachedThreadPool());
    }

    // ── Factory ───────────────────────────────────────────────────────────────

    public static RouterServer create(int port) throws IOException {
        HttpServer raw = HttpServer.create(new InetSocketAddress(port), 0);
        return new RouterServer(raw);
    }

    public RouterServer start() {
        server.start();
        return this;
    }

    // ── Route registration ────────────────────────────────────────────────────

    public void get(String path, Consumer<RequestContext> handler) {
        routes.add(new Route("GET", path, handler));
    }

    public void post(String path, Consumer<RequestContext> handler) {
        routes.add(new Route("POST", path, handler));
    }

    public void delete(String path, Consumer<RequestContext> handler) {
        routes.add(new Route("DELETE", path, handler));
    }

    // ── Lifecycle ─────────────────────────────────────────────────────────────

    /** Returns the actual bound port (useful when port 0 was requested). */
    public int port() {
        return server.getAddress().getPort();
    }

    /** Stops the server, waiting up to 1 second for in-flight requests. */
    public void stop() {
        server.stop(1);
    }

    // ── Dispatch ──────────────────────────────────────────────────────────────

    private void dispatch(HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod().toUpperCase();
        String path   = exchange.getRequestURI().getPath();

        for (Route route : routes) {
            if (route.method().equals(method) && route.path().equals(path)) {
                try {
                    route.handler().accept(new RequestContext(exchange));
                } catch (Exception ex) {
                    sendError(exchange, 500, ex.getMessage());
                }
                return;
            }
        }
        sendError(exchange, 404, "Not found: " + method + " " + path);
    }

    private static void sendError(HttpExchange ex, int status, String message) throws IOException {
        byte[] body = ("{\"error\":\"" + escape(message) + "\"}").getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", "application/json");
        ex.sendResponseHeaders(status, body.length);
        try (var os = ex.getResponseBody()) { os.write(body); }
    }

    private static String escape(String s) {
        if (s == null) return "";
        return s.replace("\\", "\\\\").replace("\"", "\\\"")
                .replace("\n", "\\n").replace("\r", "\\r");
    }

    // ── Inner types ───────────────────────────────────────────────────────────

    private record Route(String method, String path, Consumer<RequestContext> handler) {}

    /**
     * Wraps an {@link HttpExchange} and exposes Javalin-Context-like helpers
     * so the App code is easy to follow.
     */
    public static final class RequestContext {

        private final HttpExchange exchange;
        private byte[] bodyBytes;   // lazily read once

        RequestContext(HttpExchange exchange) {
            this.exchange = exchange;
        }

        // ── Request ───────────────────────────────────────────────────────────

        public String header(String name) {
            return exchange.getRequestHeaders().getFirst(name);
        }

        public String queryParam(String name) {
            String query = exchange.getRequestURI().getQuery();
            if (query == null) return null;
            for (String pair : query.split("&")) {
                int eq = pair.indexOf('=');
                if (eq < 0) continue;
                if (pair.substring(0, eq).equals(name))
                    return java.net.URLDecoder.decode(pair.substring(eq + 1), StandardCharsets.UTF_8);
            }
            return null;
        }

        public String formParam(String name) {
            // For multipart we parse the raw body; for url-encoded form treat like query params.
            String contentType = exchange.getRequestHeaders().getFirst("Content-Type");
            if (contentType != null && contentType.startsWith("multipart/form-data")) {
                return parseMultipartFormField(name);
            }
            // Try URL-encoded body
            String body = bodyAsString();
            if (body == null) return null;
            for (String pair : body.split("&")) {
                int eq = pair.indexOf('=');
                if (eq < 0) continue;
                if (pair.substring(0, eq).equals(name))
                    return java.net.URLDecoder.decode(pair.substring(eq + 1), StandardCharsets.UTF_8);
            }
            return null;
        }

        public String bodyAsString() {
            try { return new String(bodyBytes(), StandardCharsets.UTF_8); }
            catch (IOException ex) { return null; }
        }

        public <T> T bodyAsClass(Class<T> clazz) throws Exception {
            return JsonUtil.MAPPER.readValue(bodyBytes(), clazz);
        }

        /**
         * Returns the uploaded file for the given multipart field name, or {@code null}.
         * Implements enough of RFC 2046 multipart to handle a single-file upload.
         */
        public UploadedFile uploadedFile(String fieldName) {
            String contentType = exchange.getRequestHeaders().getFirst("Content-Type");
            if (contentType == null || !contentType.startsWith("multipart/form-data")) return null;
            String boundary = extractBoundary(contentType);
            if (boundary == null) return null;
            try {
                return MultipartParser.parseFile(bodyBytes(), boundary, fieldName);
            } catch (Exception ex) {
                return null;
            }
        }

        // ── Response ──────────────────────────────────────────────────────────

        private int statusCode = 200;

        public RequestContext status(int code) {
            this.statusCode = code;
            return this;
        }

        public void json(Object body) {
            try {
                byte[] bytes = JsonUtil.MAPPER.writeValueAsBytes(body);
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(statusCode, bytes.length);
                try (var os = exchange.getResponseBody()) { os.write(bytes); }
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }

        public void header(String name, String value) {
            exchange.getResponseHeaders().set(name, value);
        }

        // ── Helpers ───────────────────────────────────────────────────────────

        private byte[] bodyBytes() throws IOException {
            if (bodyBytes == null) {
                try (InputStream is = exchange.getRequestBody()) {
                    bodyBytes = is.readAllBytes();
                }
            }
            return bodyBytes;
        }

        private String parseMultipartFormField(String fieldName) {
            String contentType = exchange.getRequestHeaders().getFirst("Content-Type");
            String boundary = extractBoundary(contentType);
            if (boundary == null) return null;
            try {
                return MultipartParser.parseField(bodyBytes(), boundary, fieldName);
            } catch (Exception ex) {
                return null;
            }
        }

        private static String extractBoundary(String contentType) {
            if (contentType == null) return null;
            for (String part : contentType.split(";")) {
                String trimmed = part.trim();
                if (trimmed.startsWith("boundary=")) {
                    return trimmed.substring("boundary=".length()).trim();
                }
            }
            return null;
        }
    }

    // ── Uploaded file ─────────────────────────────────────────────────────────

    /** Minimal uploaded-file descriptor (mirrors the Javalin UploadedFile). */
    public record UploadedFile(String filename, String contentType, byte[] data) {
        public InputStream content() { return new java.io.ByteArrayInputStream(data); }
    }

    // ── JSON helper ───────────────────────────────────────────────────────────

    public static final class JsonUtil {
        public static final com.fasterxml.jackson.databind.ObjectMapper MAPPER =
                new com.fasterxml.jackson.databind.ObjectMapper();
        private JsonUtil() {}
    }

    // ── Multipart parser ──────────────────────────────────────────────────────

    /**
     * Minimal RFC 2046 multipart parser — handles a single-part file upload and
     * simple text fields.  The parser works on raw bytes so it is encoding-safe.
     */
    static final class MultipartParser {

        static UploadedFile parseFile(byte[] body, String boundary, String fieldName) {
            byte[] delimiter = ("--" + boundary).getBytes(StandardCharsets.US_ASCII);
            List<Part> parts = split(body, delimiter);
            for (Part part : parts) {
                Map<String, String> headers = parsePartHeaders(part.headers());
                String disposition = headers.get("content-disposition");
                if (disposition == null) continue;
                if (!extractDispositionParam(disposition, "name").equals(fieldName)) continue;
                String filename = extractDispositionParam(disposition, "filename");
                String ct       = headers.getOrDefault("content-type", "application/octet-stream");
                return new UploadedFile(filename, ct, part.body());
            }
            return null;
        }

        static String parseField(byte[] body, String boundary, String fieldName) {
            byte[] delimiter = ("--" + boundary).getBytes(StandardCharsets.US_ASCII);
            List<Part> parts = split(body, delimiter);
            for (Part part : parts) {
                Map<String, String> headers = parsePartHeaders(part.headers());
                String disposition = headers.get("content-disposition");
                if (disposition == null) continue;
                if (!extractDispositionParam(disposition, "name").equals(fieldName)) continue;
                return new String(part.body(), StandardCharsets.UTF_8);
            }
            return null;
        }

        private record Part(byte[] headers, byte[] body) {}

        private static List<Part> split(byte[] body, byte[] boundary) {
            List<Part> parts = new ArrayList<>();
            byte[] crlf = "\r\n".getBytes(StandardCharsets.US_ASCII);
            byte[] sep  = concat(boundary, crlf);  // "--boundary\r\n"
            int pos = 0;
            while (pos < body.length) {
                int start = indexOf(body, sep, pos);
                if (start < 0) break;
                start += sep.length;
                // Find the double CRLF that separates headers from body
                byte[] headerEnd = "\r\n\r\n".getBytes(StandardCharsets.US_ASCII);
                int bodyStart = indexOf(body, headerEnd, start);
                if (bodyStart < 0) break;
                byte[] partHeaders = copyRange(body, start, bodyStart);
                bodyStart += headerEnd.length;
                // Find the next boundary
                byte[] nextBoundary = concat(crlf, boundary);
                int bodyEnd = indexOf(body, nextBoundary, bodyStart);
                if (bodyEnd < 0) bodyEnd = body.length;
                byte[] partBody = copyRange(body, bodyStart, bodyEnd);
                parts.add(new Part(partHeaders, partBody));
                pos = bodyEnd;
            }
            return parts;
        }

        private static Map<String, String> parsePartHeaders(byte[] rawHeaders) {
            Map<String, String> map = new java.util.LinkedHashMap<>();
            String text = new String(rawHeaders, StandardCharsets.UTF_8);
            for (String line : text.split("\r\n")) {
                int colon = line.indexOf(':');
                if (colon < 0) continue;
                map.put(line.substring(0, colon).trim().toLowerCase(),
                        line.substring(colon + 1).trim());
            }
            return map;
        }

        private static String extractDispositionParam(String disposition, String param) {
            for (String part : disposition.split(";")) {
                String t = part.trim();
                if (t.startsWith(param + "=")) {
                    String val = t.substring(param.length() + 1).trim();
                    if (val.startsWith("\"") && val.endsWith("\""))
                        val = val.substring(1, val.length() - 1);
                    return val;
                }
            }
            return "";
        }

        private static int indexOf(byte[] haystack, byte[] needle, int from) {
            outer:
            for (int i = from; i <= haystack.length - needle.length; i++) {
                for (int j = 0; j < needle.length; j++) {
                    if (haystack[i + j] != needle[j]) continue outer;
                }
                return i;
            }
            return -1;
        }

        private static byte[] copyRange(byte[] src, int from, int to) {
            byte[] out = new byte[to - from];
            System.arraycopy(src, from, out, 0, out.length);
            return out;
        }

        private static byte[] concat(byte[] a, byte[] b) {
            byte[] out = new byte[a.length + b.length];
            System.arraycopy(a, 0, out, 0, a.length);
            System.arraycopy(b, 0, out, a.length, b.length);
            return out;
        }

        private MultipartParser() {}
    }
}

