package com.graphqueryengine.gremlin;

import com.graphqueryengine.App;
import io.javalin.Javalin;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test that verifies admin seed/load routes are no longer exposed
 * on the core application.
 */
class AmlCsvLoadIntegrationTest {

    private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();

    private static Javalin app;
    private static int port;

    @BeforeAll
    static void startServer() {
        app = App.start(0);
        port = app.port();
    }

    @AfterAll
    static void stopServer() {
        if (app != null) {
            app.stop();
        }
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private static String baseUrl() {
        return "http://localhost:" + port;
    }

    private HttpResponse<String> post(String endpoint) throws IOException, InterruptedException {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl() + endpoint))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();

        HttpResponse<String> resp = HTTP_CLIENT.send(req, HttpResponse.BodyHandlers.ofString());
        assertEquals(404, resp.statusCode(), "Expected route to be unavailable: " + endpoint);
        assertNotNull(resp.body(), "Expected non-null 404 response body for route: " + endpoint);
        return resp;
    }

    @Test
    void adminSeedAndLoadRoutes_areNotExposed() throws IOException, InterruptedException {
        List<String> removedRoutes = List.of(
                "/admin/seed-demo",
                "/admin/seed-10hop",
                "/admin/seed-10hop-tx",
                "/admin/seed-gremlin-10hop-tx",
                "/admin/load-aml-csv?path=/tmp/aml-demo.csv&maxRows=1"
        );

        for (String route : removedRoutes) {
            HttpResponse<String> response = post(route);
            assertNotNull(response);
        }
    }
}

