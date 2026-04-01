package com.graphqueryengine.gremlin.provider;

public final class GraphProviderFactory {
    private GraphProviderFactory() {
    }

    public static GraphProvider fromEnvironment() {
        String configuredProvider = System.getenv().getOrDefault("GRAPH_PROVIDER", "tinkergraph").trim().toLowerCase();
        return fromProviderName(configuredProvider);
    }

    public static GraphProvider fromProviderName(String configuredProvider) {
        if (configuredProvider == null || configuredProvider.isBlank() || "tinkergraph".equals(configuredProvider)) {
            return new TinkerGraphProvider();
        }
        throw new IllegalArgumentException("Unsupported GRAPH_PROVIDER: " + configuredProvider + ". Supported providers: tinkergraph");
    }
}

