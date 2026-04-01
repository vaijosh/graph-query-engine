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

    @Test
    void executesTxEndpointQuery() throws IOException, InterruptedException {
        String payload = "{\"gremlin\":\"g.V().count()\"}";

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
        assertTrue(json.get("results").get(0).asLong() >= 0L);
    }

    private static String baseUrl() {
        return "http://localhost:" + port;
    }
}

