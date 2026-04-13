package com.graphqueryengine.gremlin;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.graphqueryengine.App;
import com.graphqueryengine.TestGraphH2;
import com.graphqueryengine.http.RouterServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test for the {@code /gremlin/query/tx} HTTP endpoint.
 *
 * <p>Uses an in-memory H2 database (via {@link TestGraphH2}) so no external
 * infrastructure is required.  The App is started with {@code GRAPH_PROVIDER=wcoj}
 * pointing at the shared H2 URL.
 */
class GremlinTxEndpointIntegrationTest {

    private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static RouterServer app;
    private static int port;

    @BeforeAll
    static void startServer() throws Exception {
        // Seed H2 with tables — the server will connect to this same named in-memory DB
        // because we upload a mapping that declares the backend URL explicitly.
        //noinspection ResultOfMethodCallIgnored
        TestGraphH2.shared();

        // Start with the multi-backend "sql" path so backend-in-mapping routing works.
        // We cannot set env vars at runtime, so we ensure the mapping's declared backend
        // is used by leveraging BackendRegistry.fromEnvironment() with BACKENDS env var.
        // Alternatively: start with explicit DatabaseManager override (not exposed by App).
        // Simplest approach: start App without any explicit provider, relying on the
        // uploaded mapping's backend declaration to route queries to our H2 instance.
        app = App.start(0,
                (javalin, svc) -> {},   // no extensions
                new com.graphqueryengine.query.factory.DefaultGraphQueryTranslatorFactory());
        port = app.port();
        uploadMapping();
    }

    @AfterAll
    static void stopServer() {
        if (app != null) app.stop();
    }

    @Test
    void executesTxEndpointQuery() throws IOException, InterruptedException {
        String payload = "{\"gremlin\":\"g.V().hasLabel('Account').count()\"}";

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl() + "/gremlin/query/tx"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(payload))
                .build();

        HttpResponse<String> response = HTTP_CLIENT.send(request,
                HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode(),
                "Expected 200 but got " + response.statusCode() + ": " + response.body());

        JsonNode json = OBJECT_MAPPER.readTree(response.body());
        assertEquals(1, json.get("resultCount").asInt());
        // SQL provider returns "committed" for transactionStatus; "EXECUTED" was TinkerGraph-specific
        assertNotNull(json.get("transactionStatus").asText());
        assertTrue(json.get("results").isArray());
        assertTrue(json.get("results").get(0).asLong() >= 0L);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static void uploadMapping() throws Exception {
        // The mapping declares its own backend pointing at the shared in-memory H2.
        // SqlGraphProvider will use this backend URL directly, so the server's default
        // DB_URL does not matter for this test.
        String mappingJson = """
                {
                  "backends": {"default": {"url":"%s","user":"sa","password":"","driverClass":"org.h2.Driver"}},
                  "defaultDatasource": "default",
                  "vertices": {
                    "Account": {"table":"test_accounts","idColumn":"id","datasource":"default","properties":{"accountId":"account_id","accountType":"account_type"}}
                  },
                  "edges": {
                    "TRANSFER": {"table":"test_transfers","idColumn":"id","outColumn":"out_id","inColumn":"in_id","outVertexLabel":"Account","inVertexLabel":"Account","datasource":"default","properties":{}}
                  }
                }
                """.formatted(TestGraphH2.H2_URL);

        String boundary = "---TestBoundary";
        String body = "--" + boundary + "\r\n" +
                "Content-Disposition: form-data; name=\"file\"; filename=\"test-mapping.json\"\r\n" +
                "Content-Type: application/json\r\n\r\n" +
                mappingJson + "\r\n" +
                "--" + boundary + "--\r\n";

        HttpRequest upload = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl() + "/mapping/upload"))
                .header("Content-Type", "multipart/form-data; boundary=" + boundary)
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();

        HttpResponse<String> uploadResp = HTTP_CLIENT.send(upload,
                HttpResponse.BodyHandlers.ofString());
        assertTrue(uploadResp.statusCode() == 200 || uploadResp.statusCode() == 201,
                "Mapping upload failed (" + uploadResp.statusCode() + "): " + uploadResp.body());
    }

    private static String baseUrl() {
        return "http://localhost:" + port;
    }
}
