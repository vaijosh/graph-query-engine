package com.graphqueryengine.gremlin;

import com.graphqueryengine.gremlin.provider.GraphProvider;
import org.junit.jupiter.api.Test;

import javax.script.ScriptException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class GremlinExecutionServiceProviderTest {

    @Test
    void delegatesToInjectedProvider() throws ScriptException {
        GraphProvider provider = new GraphProvider() {
            @Override
            public String providerId() {
                return "fake-provider";
            }

            @Override
            public GremlinExecutionResult execute(String gremlin) {
                return new GremlinExecutionResult(gremlin, List.of("ok"), 1);
            }

            @Override
            public GremlinTransactionalExecutionResult executeInTransaction(String gremlin) {
                return new GremlinTransactionalExecutionResult(gremlin, List.of("ok"), 1, "FAKE", "COMMITTED");
            }

            @Override
            public void resetTransactionDemoGraph() {
            }
        };

        GremlinExecutionService service = new GremlinExecutionService(provider);

        GremlinExecutionResult result = service.execute("g.V().count()");
        assertEquals(1, result.resultCount());
        assertEquals("ok", result.results().get(0));
        assertEquals("fake-provider", service.providerId());
    }
}

