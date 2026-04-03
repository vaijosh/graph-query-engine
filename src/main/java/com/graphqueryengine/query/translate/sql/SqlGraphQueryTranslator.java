package com.graphqueryengine.query.translate.sql;

import com.graphqueryengine.mapping.MappingConfig;
import com.graphqueryengine.mapping.EdgeMapping;
import com.graphqueryengine.mapping.VertexMapping;
import com.graphqueryengine.query.api.GraphQueryTranslator;
import com.graphqueryengine.query.api.TranslationResult;
import com.graphqueryengine.query.parser.GremlinTraversalParser;
import com.graphqueryengine.query.parser.model.GremlinParseResult;
import com.graphqueryengine.query.translate.sql.dialect.IcebergSqlDialect;

import java.util.Collection;

/**
 * SQL translator pipeline:
 * parser mode (legacy/antlr) -> normalized parse model -> SQL compiler.
 */
public class SqlGraphQueryTranslator implements GraphQueryTranslator {
    public enum TranslationMode {
        STANDARD,
        ICEBERG
    }

    private final GremlinTraversalParser parser;
    private final GremlinSqlTranslator delegate;
    private final GremlinSqlTranslator icebergFallbackDelegate;
    private final TranslationMode mode;

    public SqlGraphQueryTranslator(GremlinTraversalParser parser, GremlinSqlTranslator delegate, TranslationMode mode) {
        this.parser = parser;
        this.delegate = delegate;
        this.icebergFallbackDelegate = new GremlinSqlTranslator(new IcebergSqlDialect());
        this.mode = mode == null ? TranslationMode.STANDARD : mode;
    }

    @Override
    public TranslationResult translate(String gremlin, MappingConfig mappingConfig) {
        GremlinParseResult parsed = parser.parse(gremlin);
        return switch (mode) {
            case ICEBERG -> translateIceberg(parsed, mappingConfig);
            case STANDARD -> translateStandard(parsed, mappingConfig);
        };
    }

    @Override
    public TranslationResult translateWithPlan(String gremlin, MappingConfig mappingConfig) {
        GremlinParseResult parsed = parser.parse(gremlin);
        return switch (mode) {
            case ICEBERG -> translateIcebergWithPlan(parsed, mappingConfig);
            case STANDARD -> translateStandardWithPlan(parsed, mappingConfig);
        };
    }

    protected TranslationResult translateStandard(GremlinParseResult parsed, MappingConfig mappingConfig) {
        if (looksLikeCatalogQualifiedMapping(mappingConfig)) {
            return icebergFallbackDelegate.translate(parsed, mappingConfig);
        }
        return delegate.translate(parsed, mappingConfig);
    }

    protected TranslationResult translateStandardWithPlan(GremlinParseResult parsed, MappingConfig mappingConfig) {
        if (looksLikeCatalogQualifiedMapping(mappingConfig)) {
            return icebergFallbackDelegate.translateWithPlan(parsed, mappingConfig);
        }
        return delegate.translateWithPlan(parsed, mappingConfig);
    }

    protected TranslationResult translateIceberg(GremlinParseResult parsed, MappingConfig mappingConfig) {
        return delegate.translate(parsed, mappingConfig);
    }

    protected TranslationResult translateIcebergWithPlan(GremlinParseResult parsed, MappingConfig mappingConfig) {
        return delegate.translateWithPlan(parsed, mappingConfig);
    }

    private boolean looksLikeCatalogQualifiedMapping(MappingConfig mappingConfig) {
        return hasCatalogQualifiedTable(mappingConfig.vertices().values())
                || hasCatalogQualifiedTable(mappingConfig.edges().values());
    }

    private boolean hasCatalogQualifiedTable(Collection<?> mappings) {
        for (Object mapping : mappings) {
            String table = null;
            if (mapping instanceof VertexMapping vm) {
                table = vm.table();
            } else if (mapping instanceof EdgeMapping em) {
                table = em.table();
            }
            if (table != null && table.contains(".")) {
                return true;
            }
        }
        return false;
    }
}

