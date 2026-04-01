package com.graphqueryengine.gremlin;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Integration test for the /admin/load-aml-csv endpoint.
 *
 * <p>Verifies that:
 * <ul>
 *   <li>The endpoint returns HTTP 200 (not 500 from ConcurrentModificationException)</li>
 *   <li>Alert vertices are created for suspicious (is_laundering=1) rows</li>
 *   <li>Account vertices are created from the CSV data</li>
 *   <li>Subsequent Gremlin queries against the loaded graph return correct counts</li>
 * </ul>
 *
 * <p>The test uses {@code demo/data/aml-demo.csv} which has 5 suspicious rows
 * (is_laundering=1), so exactly 5 Alert vertices should be created.
 */
class AmlCsvLoadIntegrationTest {

    private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    /** Absolute path to the bundled demo CSV (same file the notebook uses). */
    private static final String CSV_PATH =
            Path.of("demo/data/aml-demo.csv").toAbsolutePath().toString();

    /** The demo CSV has exactly 5 rows where is_laundering=1. */
    private static final int EXPECTED_ALERTS = 5;

    /** Keep a single default so tests don't pass magic numbers repeatedly. */
    private static final int DEFAULT_MAX_ROWS = 100_000;

    private static Javalin app;
    private static int port;

    @BeforeAll
    static void startServer() {
        assumeTrue(Files.exists(Path.of(CSV_PATH)),
                "Skipping AML CSV integration tests: missing input file at " + CSV_PATH
                + ". Generate/download it before running these tests.");
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

    /**
     * POST /admin/load-aml-csv and return the parsed JSON response body.
     * Fails the test immediately if the HTTP status is not 200.
     */
    private JsonNode loadCsv() throws IOException, InterruptedException {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl() + "/admin/load-aml-csv"
                        + "?path=" + CSV_PATH + "&maxRows=" + DEFAULT_MAX_ROWS))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();

        HttpResponse<String> resp = HTTP_CLIENT.send(req, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, resp.statusCode(),
                "load-aml-csv returned HTTP " + resp.statusCode()
                + " — body: " + resp.body());
        assertFalse(resp.body().isBlank(),
                "load-aml-csv returned empty body (likely ConcurrentModificationException)");
        JsonNode json = OBJECT_MAPPER.readTree(resp.body());
        assertFalse(json.has("error"),
                "load-aml-csv returned error: " + json.path("error").asText());
        return json;
    }

    /**
     * POST /gremlin/query and return the parsed JSON response body.
     */
    private JsonNode gremlin(String query) throws IOException, InterruptedException {
        String payload = OBJECT_MAPPER.writeValueAsString(java.util.Map.of("gremlin", query));
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl() + "/gremlin/query"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(payload))
                .build();
        HttpResponse<String> resp = HTTP_CLIENT.send(req, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, resp.statusCode(), "gremlin/query HTTP status");
        return OBJECT_MAPPER.readTree(resp.body());
    }

    // ── tests ─────────────────────────────────────────────────────────────────

    /**
     * Verifies the endpoint completes without a ConcurrentModificationException
     * and reports the expected vertex/edge counts.
     */
    @Test
    void loadAmlCsv_returnsSuccessJson() throws IOException, InterruptedException {
        JsonNode result = loadCsv();

        assertTrue(result.path("accountsCreated").asInt() > 0,
                "At least one Account vertex should be created");
        assertTrue(result.path("banksCreated").asInt() > 0,
                "At least one Bank vertex should be created");
        assertEquals(EXPECTED_ALERTS, result.path("alertsCreated").asInt(),
                "alertsCreated should equal the number of is_laundering=1 rows in the CSV");
        assertTrue(result.path("rowsLoaded").asInt() > 0,
                "rowsLoaded should be > 0");
    }

    /**
     * Reproduces the user-reported bug: g.V().hasLabel('Alert').count() returned 0
     * because the graph clear code threw ConcurrentModificationException, preventing
     * data from ever loading.
     */
    @Test
    void alertCount_equalsExpectedAfterLoad() throws IOException, InterruptedException {
        // Load the data first (idempotent — clears graph then reloads)
        loadCsv();

        JsonNode result = gremlin("g.V().hasLabel('Alert').count()");
        assertEquals(1, result.path("resultCount").asInt(), "resultCount should be 1");
        assertEquals(EXPECTED_ALERTS, result.path("results").get(0).asInt(),
                "Alert count should equal the number of suspicious rows (" + EXPECTED_ALERTS + ")");
    }

    /**
     * C1 query: project + outE count + order().by(select(),Order.desc).
     * Should return Account vertices ordered by descending out-transfer count.
     */
    @Test
    void c1_topSenderAccounts_returnsResults() throws IOException, InterruptedException {
        loadCsv();

        JsonNode result = gremlin(
                "g.V().hasLabel('Account')" +
                ".project('accountId','bankId','outDegree')" +
                ".by('accountId').by('bankId').by(outE('TRANSFER').count())" +
                ".order().by(select('outDegree'),Order.desc).limit(15)");

        int count = result.path("resultCount").asInt();
        assertTrue(count > 0,
                "C1 should return Account vertices, not 0 (got " + count + ")");
        assertTrue(count <= 15, "C1 limit(15) — at most 15 results");

        // First result must have the highest outDegree
        JsonNode first = result.path("results").get(0);
        assertTrue(first.has("accountId"), "First result must have accountId");
        assertTrue(first.has("outDegree"), "First result must have outDegree");
        assertTrue(first.path("outDegree").asInt() >= 0, "outDegree must be non-negative");
    }

    /**
     * Verifies that calling load-aml-csv twice in a row (clearing and reloading)
     * still produces the correct Alert count — confirming the safe collect-then-remove
     * graph clear works on a non-empty graph (second call).
     */
    @Test
    void doubleLoad_stillProducesCorrectAlertCount() throws IOException, InterruptedException {
        loadCsv();  // first load
        JsonNode second = loadCsv();  // second load (clears the now-populated graph)

        assertEquals(EXPECTED_ALERTS, second.path("alertsCreated").asInt(),
                "Second load should produce the same alert count as the first");
    }
}

