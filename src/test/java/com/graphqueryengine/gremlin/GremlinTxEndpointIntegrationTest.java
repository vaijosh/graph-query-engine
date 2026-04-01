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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GremlinTxEndpointIntegrationTest {

    private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static Javalin app;
    private static int port;

    @BeforeAll
    static void startServer() throws IOException, InterruptedException {
        app = App.start(0);
        port = app.port();

        HttpRequest seedRequest = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl() + "/admin/seed-gremlin-10hop-tx"))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();

        HttpResponse<String> seedResponse = HTTP_CLIENT.send(seedRequest, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, seedResponse.statusCode());
    }

    @AfterAll
    static void stopServer() {
        if (app != null) {
            app.stop();
        }
    }

    @Test
    void executesTxEndpointQuery() throws IOException, InterruptedException {
        String payload = "{\"gremlin\":\"g.V(1).repeat(out().simplePath()).times(10).path().limit(100)\"}";

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl() + "/gremlin/query/tx"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(payload))
                .build();

        HttpResponse<String> response = HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());

        JsonNode json = OBJECT_MAPPER.readTree(response.body());
        assertEquals(1, json.get("resultCount").asInt());
        assertEquals("EXECUTED", json.get("transactionStatus").asText());
        assertTrue(json.get("transactionMode").asText().contains("TRANSACTIONAL")
                || json.get("transactionMode").asText().contains("NON_TRANSACTIONAL"));
        assertTrue(json.get("results").isArray());
    }

    private static String baseUrl() {
        return "http://localhost:" + port;
    }
}

