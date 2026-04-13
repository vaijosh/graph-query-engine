package com.graphqueryengine.query.translate.sql.mapping;

import com.graphqueryengine.mapping.EdgeMapping;
import com.graphqueryengine.mapping.MappingConfig;
import com.graphqueryengine.mapping.VertexMapping;
import com.graphqueryengine.query.translate.sql.HasFilter;
import com.graphqueryengine.query.translate.sql.model.AsAlias;
import com.graphqueryengine.query.translate.sql.model.HopStep;
import com.graphqueryengine.query.translate.sql.constant.GremlinToken;
import com.graphqueryengine.query.translate.sql.constant.SqlFragment;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Resolves Gremlin labels and property names to their SQL table / column counterparts
 * using a {@link MappingConfig}.
 *
 * <p>All mapping look-up logic lives here; no SQL strings are produced.
 */
public class SqlMappingResolver {

    private final MappingConfig mappingConfig;

    public SqlMappingResolver(MappingConfig mappingConfig) {
        this.mappingConfig = mappingConfig;
    }

    // ── Edge mapping ──────────────────────────────────────────────────────────

    public EdgeMapping resolveEdgeMapping(String edgeLabelOrNull) {
        String resolvedLabel = edgeLabelOrNull;
        if (edgeLabelOrNull == null || edgeLabelOrNull.isBlank()) {
            resolvedLabel = resolveSingleLabel(mappingConfig.edges(), "edge");
        }
        EdgeMapping resolved = mappingConfig.edges().get(resolvedLabel);
        if (resolved == null)
            throw new IllegalArgumentException("No edge mapping found for label: " + resolvedLabel);
        return resolved;
    }

    public String resolveEdgeLabelFromFilters(List<HasFilter> filters) {
        if (mappingConfig.edges().size() == 1)
            return mappingConfig.edges().keySet().iterator().next();
        if (filters.isEmpty())
            return resolveSingleLabel(mappingConfig.edges(), "edge");

        List<String> candidates = new ArrayList<>();
        for (Map.Entry<String, EdgeMapping> entry : mappingConfig.edges().entrySet()) {
            EdgeMapping em = entry.getValue();
            boolean allMatch = filters.stream().allMatch(f ->
                    GremlinToken.PROP_ID.equals(f.property())
                    || GremlinToken.PROP_OUT_V.equals(f.property())
                    || GremlinToken.PROP_IN_V.equals(f.property())
                    || em.properties().containsKey(f.property()));
            if (allMatch) candidates.add(entry.getKey());
        }
        return candidates.size() == 1 ? candidates.get(0) : resolveSingleLabel(mappingConfig.edges(), "edge");
    }

    // ── Vertex mapping ────────────────────────────────────────────────────────

    /**
     * Resolves the target vertex of an edge traversal step.
     *
     * @param edgeMapping  the edge being traversed
     * @param outDirection {@code true} when traversing in the out-direction (i.e. towards the in-vertex)
     */
    public VertexMapping resolveTargetVertexMapping(EdgeMapping edgeMapping, boolean outDirection) {
        if (mappingConfig.vertices().size() == 1)
            return mappingConfig.vertices().values().iterator().next();

        // Explicit vertex label declared on the edge mapping takes priority
        String explicitLabel = outDirection ? edgeMapping.inVertexLabel() : edgeMapping.outVertexLabel();
        if (explicitLabel != null && !explicitLabel.isBlank()) {
            VertexMapping vm = mappingConfig.vertices().get(explicitLabel);
            if (vm != null) return vm;
        }

        String targetCol = outDirection ? edgeMapping.inColumn() : edgeMapping.outColumn();
        String stem = targetCol.replaceAll("_id$", "").replaceAll("^.*_", "");

        for (Map.Entry<String, VertexMapping> entry : mappingConfig.vertices().entrySet()) {
            String rawTable = entry.getValue().table();
            String unqualified = rawTable.contains(".")
                    ? rawTable.substring(rawTable.lastIndexOf('.') + 1)
                    : rawTable.replaceAll("^[a-z]+_", "");
            if (unqualified.equals(stem) || unqualified.startsWith(stem)) return entry.getValue();
        }
        for (Map.Entry<String, VertexMapping> entry : mappingConfig.vertices().entrySet()) {
            if (entry.getKey().equalsIgnoreCase(stem)) return entry.getValue();
        }
        for (VertexMapping vm : mappingConfig.vertices().values()) {
            if (targetCol.equals(vm.idColumn())) return vm;
        }

        String edgeTable = edgeMapping.table();
        String edgeTableBase = edgeTable.contains(".")
                ? edgeTable.substring(edgeTable.lastIndexOf('.') + 1) : edgeTable;
        String[] edgeParts = edgeTableBase.split("_");
        String edgeStem = outDirection ? edgeParts[edgeParts.length - 1] : edgeParts[0];
        for (Map.Entry<String, VertexMapping> entry : mappingConfig.vertices().entrySet()) {
            if (entry.getKey().equalsIgnoreCase(edgeStem)) return entry.getValue();
        }
        for (Map.Entry<String, VertexMapping> entry : mappingConfig.vertices().entrySet()) {
            String rawTable = entry.getValue().table();
            String unqualified = rawTable.contains(".")
                    ? rawTable.substring(rawTable.lastIndexOf('.') + 1)
                    : rawTable.replaceAll("^[a-z]+_", "");
            if (unqualified.startsWith(edgeStem)) return entry.getValue();
        }
        return null;
    }

    /**
     * Like {@link #resolveTargetVertexMapping} but falls back to {@code defaultMapping}
     * when no explicit mapping is found.
     */
    public VertexMapping resolveHopTargetVertexMapping(EdgeMapping edgeMapping, boolean outDirection,
                                                        VertexMapping defaultMapping) {
        VertexMapping resolved = resolveTargetVertexMapping(edgeMapping, outDirection);
        return resolved != null ? resolved : defaultMapping;
    }

    /** Walks the hop list and returns the vertex mapping at the end of the traversal. */
    public VertexMapping resolveFinalHopVertexMapping(List<HopStep> hops, VertexMapping startVertexMapping) {
        VertexMapping current = startVertexMapping;
        for (HopStep hop : hops) {
            if (!GremlinToken.OUT_V.equals(hop.direction()) && !GremlinToken.IN_V.equals(hop.direction())
                    && !GremlinToken.OUT_E.equals(hop.direction()) && !GremlinToken.IN_E.equals(hop.direction())
                    && !GremlinToken.BOTH_E.equals(hop.direction())) {
                EdgeMapping em = resolveEdgeMapping(hop.singleLabel());
                current = resolveHopTargetVertexMapping(em, GremlinToken.OUT.equals(hop.direction()), current);
            }
        }
        return current;
    }

    /**
     * Returns the vertex mapping reached after {@code hopIndex} hops from {@code startVertexMapping}.
     */
    public VertexMapping resolveVertexMappingAtHop(List<HopStep> hops, VertexMapping startVertexMapping,
                                                    int hopIndex) {
        VertexMapping current = startVertexMapping;
        for (int i = 0; i < Math.min(hopIndex, hops.size()); i++) {
            HopStep hop = hops.get(i);
            if (!GremlinToken.OUT_V.equals(hop.direction()) && !GremlinToken.IN_V.equals(hop.direction())
                    && !GremlinToken.OUT_E.equals(hop.direction()) && !GremlinToken.IN_E.equals(hop.direction())) {
                EdgeMapping em = resolveEdgeMapping(hop.singleLabel());
                current = resolveHopTargetVertexMapping(em, GremlinToken.OUT.equals(hop.direction()), current);
            }
        }
        return current;
    }

    /** Returns the single vertex mapping when there is exactly one, or {@code null}. */
    public VertexMapping resolveVertexMappingForEdgeProjection() {
        return mappingConfig.vertices().size() == 1
                ? mappingConfig.vertices().values().iterator().next() : null;
    }

    /** Returns the first vertex mapping whose property set contains all of {@code propertyNames}. */
    public VertexMapping resolveVertexMappingByProperties(List<String> propertyNames) {
        if (propertyNames.isEmpty()) return null;
        for (VertexMapping vm : mappingConfig.vertices().values()) {
            if (propertyNames.stream().allMatch(p -> vm.properties().containsKey(p))) return vm;
        }
        return null;
    }

    /**
     * Returns the vertex mapping that uniquely contains {@code propertyName},
     * or {@code null} if zero or more than one mapping contains it.
     */
    public VertexMapping resolveUniqueVertexMappingByProperty(String propertyName) {
        List<VertexMapping> matches = new ArrayList<>();
        for (VertexMapping vm : mappingConfig.vertices().values()) {
            if (vm.properties().containsKey(propertyName)) matches.add(vm);
        }
        return matches.size() == 1 ? matches.get(0) : null;
    }

    // ── Property / column mapping ─────────────────────────────────────────────

    public String mapVertexProperty(VertexMapping mapping, String property) {
        String column = mapping.properties().get(property);
        if (column == null)
            throw new IllegalArgumentException("No vertex property mapping found for property: " + property);
        return column;
    }

    public String mapEdgeProperty(EdgeMapping mapping, String property) {
        String column = mapping.properties().get(property);
        if (column == null)
            throw new IllegalArgumentException("No edge property mapping found for property: " + property);
        return column;
    }

    public String mapEdgeFilterProperty(EdgeMapping mapping, String property) {
        if (GremlinToken.PROP_ID.equals(property))    return mapping.idColumn();
        if (GremlinToken.PROP_OUT_V.equals(property)) return mapping.outColumn();
        if (GremlinToken.PROP_IN_V.equals(property))  return mapping.inColumn();
        return mapEdgeProperty(mapping, property);
    }

    // ── Alias resolution ──────────────────────────────────────────────────────

    public int resolveAliasHopIndex(String aliasLabel, List<AsAlias> asAliases, String side) {
        for (AsAlias alias : asAliases) {
            if (alias.label().equals(aliasLabel)) return alias.hopIndexAfter();
        }
        throw new IllegalArgumentException("where() " + side + " alias not found: " + aliasLabel);
    }

    public String resolveVertexAliasForWhere(String aliasLabel, Map<String, Integer> aliasHopIndex) {
        Integer hopIndex = aliasHopIndex.get(aliasLabel);
        if (hopIndex == null)
            throw new IllegalArgumentException("where() alias not found: " + aliasLabel);
        return SqlFragment.V_PREFIX + hopIndex;
    }

    // ── Label resolution ──────────────────────────────────────────────────────

    public String resolveSingleLabel(Map<String, ?> map, String kind) {
        if (map.size() != 1)
            throw new IllegalArgumentException(
                    "Traversal must include hasLabel() when more than one " + kind + " label is mapped");
        return map.keySet().iterator().next();
    }

    public MappingConfig mappingConfig() {
        return mappingConfig;
    }
}

