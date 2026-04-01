package com.graphqueryengine.query;

import com.graphqueryengine.mapping.EdgeMapping;
import com.graphqueryengine.mapping.MappingConfig;
import com.graphqueryengine.mapping.VertexMapping;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GremlinSqlTranslator {
    private static final Pattern ENDPOINT_VALUES_PATTERN = Pattern.compile("^(outV|inV)\\(\\)\\.values\\((.+)\\)$");

    // Matches: out('L1','L2').values('prop').fold()  or  in('L1').values('prop').fold()
    private static final Pattern NEIGHBOR_VALUES_FOLD_PATTERN =
            Pattern.compile("^(out|in)\\((.+)\\)\\.values\\(['\"]([^'\"]+)['\"]\\)\\.fold\\(\\)$");

    public TranslationResult translate(String gremlin, MappingConfig mappingConfig) {
        if (gremlin == null || gremlin.isBlank()) {
            throw new IllegalArgumentException("Gremlin query is required");
        }

        String query = gremlin.trim();
        if (!query.startsWith("g.")) {
            throw new IllegalArgumentException("Gremlin query must start with g.");
        }

        RootStep rootStep = parseRootStep(query);
        ParsedTraversal parsedTraversal = parseSteps(query, rootStep.nextIndex(), rootStep.rootIdFilter());
        return rootStep.vertexQuery() ? buildVertexSql(parsedTraversal, mappingConfig) : buildEdgeSql(parsedTraversal, mappingConfig);
    }

    private RootStep parseRootStep(String query) {
        if (query.startsWith("g.V(")) {
            ParsedArgs parsedArgs = readArgs(query, "g.V".length());
            List<String> args = splitArgs(parsedArgs.args());
            if (args.size() > 1) {
                throw new IllegalArgumentException("g.V() expects 0 or 1 argument(s)");
            }
            String rootIdFilter = args.isEmpty() || args.get(0).isBlank() ? null : unquote(args.get(0));
            return new RootStep(true, parsedArgs.nextIndex(), rootIdFilter);
        }

        if (query.startsWith("g.E(")) {
            ParsedArgs parsedArgs = readArgs(query, "g.E".length());
            List<String> args = splitArgs(parsedArgs.args());
            if (args.size() > 1) {
                throw new IllegalArgumentException("g.E() expects 0 or 1 argument(s)");
            }
            return new RootStep(false, parsedArgs.nextIndex(), null);
        }

        throw new IllegalArgumentException("Only g.V(...) and g.E(...) traversals are supported");
    }

    private TranslationResult buildVertexSql(ParsedTraversal parsed, MappingConfig mappingConfig) {
        var ref = new Object() {
            String label = parsed.label();
        };
        if (ref.label == null) {
            ref.label = resolveSingleLabel(mappingConfig.vertices(), "vertex");
        }

        VertexMapping vertexMapping = Optional.ofNullable(mappingConfig.vertices().get(ref.label))
                .orElseThrow(() -> new IllegalArgumentException("No vertex mapping found for label: " + ref.label));

        // For non-hop paths, treat preHopLimit as the effective limit
        Integer effectiveLimit = parsed.limit() != null ? parsed.limit() : parsed.preHopLimit();

        if (parsed.groupCountProperty() != null) {
            return buildVertexGroupCountSql(parsed, vertexMapping);
        }

        // Hop queries (including both()) must be routed before projection handling
        if (!parsed.hops().isEmpty()) {
            return buildHopTraversalSql(parsed, mappingConfig, vertexMapping);
        }

        if (!parsed.projections().isEmpty()) {
            return buildVertexProjectionSql(parsed, mappingConfig, vertexMapping);
        }

        String selectClause = parsed.countRequested()
                ? (parsed.dedupRequested() ? "COUNT(DISTINCT " + vertexMapping.idColumn() + ") AS count" : "COUNT(*) AS count")
                : (parsed.dedupRequested() ? "DISTINCT *" : "*");
        List<Object> params = new ArrayList<>();
        if (parsed.valueProperty() != null && !parsed.countRequested()) {
            String mappedColumn = mapVertexProperty(vertexMapping, parsed.valueProperty());
            selectClause = (parsed.dedupRequested() ? "DISTINCT " : "") + mappedColumn + " AS " + parsed.valueProperty();
        }


        StringBuilder sql = new StringBuilder("SELECT ").append(selectClause)
                .append(" FROM ").append(vertexMapping.table());

        appendWhereClauseForVertex(sql, params, parsed.filters(), vertexMapping);
        if (parsed.whereClause() != null) {
            if (parsed.whereClause().kind() != WhereKind.EDGE_EXISTS
                    && parsed.whereClause().kind() != WhereKind.OUT_NEIGHBOR_HAS
                    && parsed.whereClause().kind() != WhereKind.IN_NEIGHBOR_HAS) {
                throw new IllegalArgumentException("Unsupported where() predicate for g.V(): " + parsed.whereClause().kind());
            }
            if (parsed.whereClause().kind() == WhereKind.EDGE_EXISTS) {
                appendEdgeExistsPredicate(sql, params, parsed.whereClause(), mappingConfig, vertexMapping, null, !parsed.filters().isEmpty());
            } else {
                appendNeighborHasPredicate(sql, params, parsed.whereClause(), mappingConfig, vertexMapping, null, !parsed.filters().isEmpty());
            }
        }
        appendOrderBy(sql, parsed.orderByProperty(), parsed.orderDirection());
        appendLimit(sql, effectiveLimit);

        return new TranslationResult(sql.toString(), params);
    }

    private TranslationResult buildHopTraversalSql(ParsedTraversal parsed, MappingConfig mappingConfig, VertexMapping startVertexMapping) {
        List<Object> params = new ArrayList<>();

        // Detect if any hop uses both() — requires UNION treatment
        boolean hasBothHop = parsed.hops().stream().anyMatch(h -> "both".equals(h.direction()));
        if (hasBothHop) {
            return buildBothHopUnionSql(parsed, mappingConfig, startVertexMapping);
        }

        // Detect if any hop carries multiple edge labels — requires UNION treatment
        boolean hasMultiLabelHop = parsed.hops().stream().anyMatch(h -> h.labels().size() > 1);
        if (hasMultiLabelHop) {
            return buildMultiLabelHopUnionSql(parsed, mappingConfig, startVertexMapping);
        }

        StringBuilder sql = new StringBuilder();
        int hopCount = parsed.hops().size();
        String finalVertexAlias = "v" + hopCount;
        String idCol = startVertexMapping.idColumn();

        // SELECT clause
        if (parsed.countRequested()) {
            if (parsed.dedupRequested()) {
                sql.append("SELECT COUNT(DISTINCT ").append(finalVertexAlias).append('.').append(idCol).append(") AS count");
            } else {
                sql.append("SELECT COUNT(*) AS count");
            }
        } else if (!parsed.projections().isEmpty()) {
            StringJoiner projectionSelect = new StringJoiner(", ");
            for (ProjectionField projection : parsed.projections()) {
                if (projection.kind() != ProjectionKind.EDGE_PROPERTY) {
                    throw new IllegalArgumentException("Hop projection currently supports by('property') only");
                }
                String mappedColumn = mapVertexProperty(startVertexMapping, projection.property());
                projectionSelect.add(finalVertexAlias + "." + mappedColumn + " AS " + quoteAlias(projection.alias()));
            }
            sql.append(parsed.dedupRequested() ? "SELECT DISTINCT " : "SELECT ").append(projectionSelect);
        } else if (parsed.pathSeen() && !parsed.pathByProperties().isEmpty()) {
            // path().by('p0').by('p1')... — one by() per hop vertex v0..vN
            sql.append(buildPathSelectClause(parsed.pathByProperties(), hopCount, mappingConfig, startVertexMapping, parsed.hops()));
        } else if (parsed.valueProperty() != null) {
            String mappedColumn = mapVertexProperty(startVertexMapping, parsed.valueProperty());
            sql.append("SELECT ")
                    .append(parsed.dedupRequested() ? "DISTINCT " : "")
                    .append(finalVertexAlias).append('.').append(mappedColumn)
                    .append(" AS ").append(parsed.valueProperty());
        } else {
            sql.append(parsed.dedupRequested() ? "SELECT DISTINCT " : "SELECT ").append(finalVertexAlias).append(".*");
        }

        // FROM clause — always wrap starting vertex in a subquery when preHopLimit is set.
        // This guarantees LIMIT appears inside the subquery (before any JOINs/WHERE on v0),
        // preventing the invalid SQL pattern "… WHERE … LIMIT n AND …".
        boolean useStartSubquery = parsed.preHopLimit() != null;

        if (useStartSubquery) {
            StringBuilder startSubquery = new StringBuilder("(SELECT * FROM ").append(startVertexMapping.table());
            List<Object> subParams = new ArrayList<>();
            appendWhereClauseForVertex(startSubquery, subParams, parsed.filters(), startVertexMapping);
            if (parsed.whereClause() != null) {
                if (parsed.whereClause().kind() == WhereKind.EDGE_EXISTS) {
                    appendEdgeExistsPredicate(startSubquery, subParams, parsed.whereClause(), mappingConfig, startVertexMapping, null, !parsed.filters().isEmpty());
                } else if (parsed.whereClause().kind() == WhereKind.OUT_NEIGHBOR_HAS || parsed.whereClause().kind() == WhereKind.IN_NEIGHBOR_HAS) {
                    appendNeighborHasPredicate(startSubquery, subParams, parsed.whereClause(), mappingConfig, startVertexMapping, null, !parsed.filters().isEmpty());
                } else {
                    throw new IllegalArgumentException("Unsupported where() predicate for hop traversal: " + parsed.whereClause().kind());
                }
            }
            startSubquery.append(" LIMIT ").append(parsed.preHopLimit()).append(")");
            params.addAll(subParams);
            sql.append(" FROM ").append(startSubquery).append(" v0");
        } else {
            sql.append(" FROM ").append(startVertexMapping.table()).append(" v0");
        }

        // JOIN hops
        for (int i = 0; i < hopCount; i++) {
            HopStep hop = parsed.hops().get(i);
            EdgeMapping edgeMapping = resolveEdgeMapping(hop.singleLabel(), mappingConfig);
            String edgeAlias = "e" + (i + 1);
            String fromVertexAlias = "v" + i;
            String toVertexAlias = "v" + (i + 1);
            String fromId = fromVertexAlias + "." + idCol;
            String toId = toVertexAlias + "." + idCol;
            String edgeOut = edgeAlias + "." + edgeMapping.outColumn();
            String edgeIn = edgeAlias + "." + edgeMapping.inColumn();

            sql.append(" JOIN ").append(edgeMapping.table()).append(' ').append(edgeAlias);
            if ("out".equals(hop.direction())) {
                sql.append(" ON ").append(edgeOut).append(" = ").append(fromId);
            } else {
                sql.append(" ON ").append(edgeIn).append(" = ").append(fromId);
            }

            // Resolve the destination vertex table for this hop
            VertexMapping toVertexMapping = resolveHopTargetVertexMapping(mappingConfig, edgeMapping, "out".equals(hop.direction()), startVertexMapping);
            sql.append(" JOIN ").append(toVertexMapping.table()).append(' ').append(toVertexAlias);
            if ("out".equals(hop.direction())) {
                sql.append(" ON ").append(toId).append(" = ").append(edgeIn);
            } else {
                sql.append(" ON ").append(toId).append(" = ").append(edgeOut);
            }
        }

        // WHERE clause — filters/where on v0 only when NOT using the start subquery
        // (filters are baked into the subquery when useStartSubquery=true).
        boolean hasWhere = false;
        if (!useStartSubquery) {
            if (!parsed.filters().isEmpty()) {
                appendWhereClauseForVertexAlias(sql, params, parsed.filters(), startVertexMapping, "v0");
                hasWhere = true;
            }
            if (parsed.whereClause() != null) {
                if (parsed.whereClause().kind() == WhereKind.EDGE_EXISTS) {
                    appendEdgeExistsPredicate(sql, params, parsed.whereClause(), mappingConfig, startVertexMapping, "v0", hasWhere);
                } else if (parsed.whereClause().kind() == WhereKind.OUT_NEIGHBOR_HAS || parsed.whereClause().kind() == WhereKind.IN_NEIGHBOR_HAS) {
                    appendNeighborHasPredicate(sql, params, parsed.whereClause(), mappingConfig, startVertexMapping, "v0", hasWhere);
                } else {
                    throw new IllegalArgumentException("Unsupported where() predicate for hop traversal: " + parsed.whereClause().kind());
                }
                hasWhere = true;
            }
        }

        // simplePath() cycle-detection: each vertex must not equal any prior vertex
        // v1 != v0, v2 NOT IN (v0,v1), v3 NOT IN (v0,v1,v2), ...
        if (parsed.simplePathRequested() && hopCount > 0) {
            StringJoiner cycleConditions = new StringJoiner(" AND ");
            for (int i = 1; i <= hopCount; i++) {
                String currentAlias = "v" + i + "." + idCol;
                if (i == 1) {
                    cycleConditions.add(currentAlias + " <> v0." + idCol);
                } else {
                    StringJoiner priorAliases = new StringJoiner(", ");
                    for (int j = 0; j < i; j++) {
                        priorAliases.add("v" + j + "." + idCol);
                    }
                    cycleConditions.add(currentAlias + " NOT IN (" + priorAliases + ")");
                }
            }
            sql.append(hasWhere ? " AND " : " WHERE ").append(cycleConditions);
        }

        appendLimit(sql, parsed.limit());

        return new TranslationResult(sql.toString(), params);
    }

    /**
     * Builds a path SELECT clause for path().by('p0').by('p1')... traversals where each by()
     * maps to the vertex at position i in v0..vN. When there are fewer by() modulators than
     * vertices, the last modulator is repeated (Gremlin cycling semantics).
     * Each vertex may live in a different table, so each hop's target mapping is used.
     */
    private String buildPathSelectClause(List<String> pathByProps, int hopCount,
                                         MappingConfig mappingConfig, VertexMapping startVertexMapping,
                                         List<HopStep> hops) {
        StringJoiner pathSelect = new StringJoiner(", ");
        // Collect vertex mappings per hop: v0 = startVertexMapping, v1..vN from hop targets
        List<VertexMapping> hopMappings = new ArrayList<>();
        hopMappings.add(startVertexMapping);
        VertexMapping prev = startVertexMapping;
        for (HopStep hop : hops) {
            if (hop.labels().isEmpty()) {
                hopMappings.add(prev);
            } else {
                // Use the first label for target resolution (primary path for multi-label hops)
                EdgeMapping em = resolveEdgeMapping(hop.labels().get(0), mappingConfig);
                VertexMapping target = resolveHopTargetVertexMapping(mappingConfig, em, "out".equals(hop.direction()), prev);
                hopMappings.add(target);
                prev = target;
            }
        }

        int numProps = pathByProps.size();
        for (int i = 0; i <= hopCount; i++) {
            String prop = pathByProps.get(Math.min(i, numProps - 1));
            VertexMapping vm = hopMappings.get(Math.min(i, hopMappings.size() - 1));
            String mappedColumn = vm.properties().get(prop);
            if (mappedColumn == null) {
                // Property not on expected hop vertex — search all vertex mappings for a match
                for (VertexMapping candidate : mappingConfig.vertices().values()) {
                    if (candidate.properties().containsKey(prop)) {
                        mappedColumn = candidate.properties().get(prop);
                        break;
                    }
                }
            }
            if (mappedColumn != null) {
                pathSelect.add("v" + i + "." + mappedColumn + " AS " + prop + i);
            } else {
                pathSelect.add("NULL AS " + prop + i);
            }
        }
        return "SELECT " + pathSelect;
    }

    /**
     * Resolves the vertex mapping for the destination of a single hop edge traversal.
     * Falls back to {@code defaultMapping} when no better match is found.
     */
    private VertexMapping resolveHopTargetVertexMapping(MappingConfig mappingConfig,
                                                        EdgeMapping edgeMapping,
                                                        boolean outDirection,
                                                        VertexMapping defaultMapping) {
        VertexMapping resolved = resolveTargetVertexMapping(mappingConfig, edgeMapping, outDirection);
        return resolved != null ? resolved : defaultMapping;
    }

    /**
     * Handles hop traversals where at least one step carries multiple edge labels
     * (e.g. {@code out('BELONGS_TO','LOCATED_IN')}).
     * <p>
     * <b>Strategy:</b>
     * <ul>
     *   <li>Single-hop multi-label (e.g. {@code .out('A','B')}): generates a UNION of one
     *       branch per label. This is the exact Gremlin semantics — traverse via A <em>or</em> B.</li>
     *   <li>Multi-hop multi-label (e.g. {@code repeat(out('A','B')).times(2)}): uses a
     *       schema-aware linear LEFT JOIN chain, picking the valid edge for each hop based on
     *       which label actually connects the current source vertex type. This avoids the
     *       O(labels^hops) UNION explosion while still covering heterogeneous hop sequences
     *       (e.g. Account→Bank via BELONGS_TO, then Bank→Country via LOCATED_IN).</li>
     * </ul>
     */
    private TranslationResult buildMultiLabelHopUnionSql(ParsedTraversal parsed,
                                                         MappingConfig mappingConfig,
                                                         VertexMapping startVertexMapping) {
        int hopCount = parsed.hops().size();

        // Single-hop multi-label: use a UNION of one branch per label.
        // This is simple, correct, and avoids schema-resolution guesswork for ambiguous edges.
        if (hopCount == 1) {
            HopStep hop = parsed.hops().get(0);
            List<String> labels = hop.labels();
            List<String> branchSqls = new ArrayList<>();
            List<Object> allParams = new ArrayList<>();
            for (String label : labels) {
                List<HopStep> singleHop = List.of(new HopStep(hop.direction(), label));
                ParsedTraversal singleParsed = withHops(parsed, singleHop);
                TranslationResult br = buildHopTraversalSql(singleParsed, mappingConfig, startVertexMapping);
                branchSqls.add(br.sql());
                allParams.addAll(br.parameters());
            }
            String unionKeyword = parsed.dedupRequested() ? " UNION " : " UNION ALL ";
            StringBuilder finalSql = new StringBuilder(String.join(unionKeyword, branchSqls));
            appendLimit(finalSql, parsed.limit());
            return new TranslationResult(finalSql.toString(), allParams);
        }

        // Multi-hop multi-label: schema-aware linear LEFT JOIN chain.
        // Walk hops sequentially; at each hop, pick the label(s) that connect the current
        // source vertex type. When one label is valid, emit an INNER JOIN. When multiple
        // labels could connect (or the first is the primary), emit JOIN for the first and
        // LEFT JOINs for the others, advancing the vertex alias from the primary path.
        List<Object> params = new ArrayList<>();
        StringBuilder sql = new StringBuilder();
        String idCol = startVertexMapping.idColumn();
        String finalVertexAlias = "v" + hopCount;

        // SELECT clause
        if (parsed.countRequested()) {
            sql.append(parsed.dedupRequested()
                    ? "SELECT COUNT(DISTINCT " + finalVertexAlias + "." + idCol + ") AS count"
                    : "SELECT COUNT(*) AS count");
        } else if (!parsed.projections().isEmpty()) {
            StringJoiner pj = new StringJoiner(", ");
            for (ProjectionField pf : parsed.projections()) {
                if (pf.kind() != ProjectionKind.EDGE_PROPERTY)
                    throw new IllegalArgumentException("Hop projection currently supports by('property') only");
                pj.add(finalVertexAlias + "." + mapVertexProperty(startVertexMapping, pf.property()) + " AS " + quoteAlias(pf.alias()));
            }
            sql.append(parsed.dedupRequested() ? "SELECT DISTINCT " : "SELECT ").append(pj);
        } else if (parsed.pathSeen() && !parsed.pathByProperties().isEmpty()) {
            sql.append(buildPathSelectClause(parsed.pathByProperties(), hopCount, mappingConfig, startVertexMapping, parsed.hops()));
        } else if (parsed.valueProperty() != null) {
            // Find the property on the last hop's target vertex mapping
            VertexMapping lastVm = startVertexMapping;
            for (HopStep hop : parsed.hops()) {
                if (!hop.labels().isEmpty()) {
                    EdgeMapping em = resolveEdgeMapping(hop.labels().get(0), mappingConfig);
                    lastVm = resolveHopTargetVertexMapping(mappingConfig, em, "out".equals(hop.direction()), lastVm);
                }
            }
            String col = lastVm.properties().get(parsed.valueProperty());
            if (col == null) {
                for (VertexMapping vm : mappingConfig.vertices().values()) {
                    if (vm.properties().containsKey(parsed.valueProperty())) { col = vm.properties().get(parsed.valueProperty()); break; }
                }
            }
            if (col == null) throw new IllegalArgumentException("No vertex property mapping found for property: " + parsed.valueProperty());
            sql.append("SELECT ").append(parsed.dedupRequested() ? "DISTINCT " : "")
               .append(finalVertexAlias).append('.').append(col).append(" AS ").append(parsed.valueProperty());
        } else {
            sql.append(parsed.dedupRequested() ? "SELECT DISTINCT " : "SELECT ").append(finalVertexAlias).append(".*");
        }

        // FROM clause
        boolean useStartSubquery = parsed.preHopLimit() != null;
        if (useStartSubquery) {
            StringBuilder sq = new StringBuilder("(SELECT * FROM ").append(startVertexMapping.table());
            List<Object> sqp = new ArrayList<>();
            appendWhereClauseForVertex(sq, sqp, parsed.filters(), startVertexMapping);
            if (parsed.whereClause() != null) {
                if (parsed.whereClause().kind() == WhereKind.EDGE_EXISTS)
                    appendEdgeExistsPredicate(sq, sqp, parsed.whereClause(), mappingConfig, startVertexMapping, null, !parsed.filters().isEmpty());
                else if (parsed.whereClause().kind() == WhereKind.OUT_NEIGHBOR_HAS || parsed.whereClause().kind() == WhereKind.IN_NEIGHBOR_HAS)
                    appendNeighborHasPredicate(sq, sqp, parsed.whereClause(), mappingConfig, startVertexMapping, null, !parsed.filters().isEmpty());
            }
            sq.append(" LIMIT ").append(parsed.preHopLimit()).append(")");
            params.addAll(sqp);
            sql.append(" FROM ").append(sq).append(" v0");
        } else {
            sql.append(" FROM ").append(startVertexMapping.table()).append(" v0");
        }

        // Linear LEFT JOIN chain — one set of joins per hop
        VertexMapping currentSource = startVertexMapping;
        for (int i = 0; i < hopCount; i++) {
            HopStep hop = parsed.hops().get(i);
            List<String> labels = hop.labels();
            String dir = hop.direction();
            String toAlias = "v" + (i + 1);
            String fromId = "v" + i + "." + idCol;
            String toId = toAlias + "." + idCol;

            // Primary label (first): determines the main vertex alias for next hop
            EdgeMapping primaryEdge = resolveEdgeMapping(labels.get(0), mappingConfig);
            VertexMapping primaryTarget = resolveHopTargetVertexMapping(mappingConfig, primaryEdge, "out".equals(dir), currentSource);
            String eAlias = "e" + (i + 1);
            sql.append(" JOIN ").append(primaryEdge.table()).append(' ').append(eAlias);
            sql.append(" ON ").append(eAlias).append('.')
               .append("out".equals(dir) ? primaryEdge.outColumn() : primaryEdge.inColumn())
               .append(" = ").append(fromId);
            sql.append(" JOIN ").append(primaryTarget.table()).append(' ').append(toAlias);
            sql.append(" ON ").append(toId).append(" = ").append(eAlias).append('.')
               .append("out".equals(dir) ? primaryEdge.inColumn() : primaryEdge.outColumn());

            // Additional labels: LEFT JOINs (optional paths via alternate edges)
            for (int l = 1; l < labels.size(); l++) {
                EdgeMapping altEdge = resolveEdgeMapping(labels.get(l), mappingConfig);
                VertexMapping altTarget = resolveHopTargetVertexMapping(mappingConfig, altEdge, "out".equals(dir), currentSource);
                String altEAlias = "e" + (i + 1) + "_" + l;
                String altVAlias = "vt" + (i + 1) + "_" + l;
                sql.append(" LEFT JOIN ").append(altEdge.table()).append(' ').append(altEAlias);
                sql.append(" ON ").append(altEAlias).append('.')
                   .append("out".equals(dir) ? altEdge.outColumn() : altEdge.inColumn())
                   .append(" = ").append(fromId);
                sql.append(" LEFT JOIN ").append(altTarget.table()).append(' ').append(altVAlias);
                sql.append(" ON ").append(altVAlias).append('.').append(altTarget.idColumn())
                   .append(" = ").append(altEAlias).append('.')
                   .append("out".equals(dir) ? altEdge.inColumn() : altEdge.outColumn());
            }
            currentSource = primaryTarget;
        }

        // WHERE clause
        boolean hasWhere = false;
        if (!useStartSubquery) {
            if (!parsed.filters().isEmpty()) {
                appendWhereClauseForVertexAlias(sql, params, parsed.filters(), startVertexMapping, "v0");
                hasWhere = true;
            }
            if (parsed.whereClause() != null) {
                if (parsed.whereClause().kind() == WhereKind.EDGE_EXISTS)
                    appendEdgeExistsPredicate(sql, params, parsed.whereClause(), mappingConfig, startVertexMapping, "v0", hasWhere);
                else if (parsed.whereClause().kind() == WhereKind.OUT_NEIGHBOR_HAS || parsed.whereClause().kind() == WhereKind.IN_NEIGHBOR_HAS)
                    appendNeighborHasPredicate(sql, params, parsed.whereClause(), mappingConfig, startVertexMapping, "v0", hasWhere);
                hasWhere = true;
            }
        }

        // simplePath() cycle-detection
        if (parsed.simplePathRequested() && hopCount > 0) {
            StringJoiner cc = new StringJoiner(" AND ");
            for (int i = 1; i <= hopCount; i++) {
                String cur = "v" + i + "." + idCol;
                if (i == 1) {
                    cc.add(cur + " <> v0." + idCol);
                } else {
                    StringJoiner prior = new StringJoiner(", ");
                    for (int j = 0; j < i; j++) prior.add("v" + j + "." + idCol);
                    cc.add(cur + " NOT IN (" + prior + ")");
                }
            }
            sql.append(hasWhere ? " AND " : " WHERE ").append(cc);
        }

        appendLimit(sql, parsed.limit());
        return new TranslationResult(sql.toString(), params);
    }

    /** Creates a copy of {@code parsed} with a different hops list (all other fields identical). */
    private ParsedTraversal withHops(ParsedTraversal parsed, List<HopStep> newHops) {
        return new ParsedTraversal(
                parsed.label(), parsed.filters(), parsed.valueProperty(), parsed.limit(),
                parsed.preHopLimit(), newHops, parsed.countRequested(), parsed.projections(),
                parsed.groupCountProperty(), parsed.orderByProperty(), parsed.orderDirection(),
                parsed.asAliases(), parsed.selectFields(), parsed.whereClause(),
                parsed.dedupRequested(), parsed.pathSeen(), parsed.pathByProperties(),
                parsed.simplePathRequested());
    }


    /**
     * Handles hop traversals containing a both() step by generating a UNION of
     * outgoing and incoming edge branches, matching Gremlin's bidirectional semantics.
     *
     * g.V().where(...).limit(1).both('TRANSFER').dedup().project(...)
     * →
     * SELECT ... FROM (...start...) v0
     *   JOIN aml_transfers e1 ON e1.out_id = v0.id JOIN aml_accounts v1 ON v1.id = e1.in_id
     * UNION
     * SELECT ... FROM (...start...) v0
     *   JOIN aml_transfers e1 ON e1.in_id = v0.id JOIN aml_accounts v1 ON v1.id = e1.out_id
     */
    private TranslationResult buildBothHopUnionSql(ParsedTraversal parsed, MappingConfig mappingConfig, VertexMapping startVertexMapping) {
        int hopCount = parsed.hops().size();
        String idCol = startVertexMapping.idColumn();
        String finalVertexAlias = "v" + hopCount;

        String selectClause;
        if (parsed.countRequested()) {
            // wrap in outer COUNT after UNION to avoid double-counting
            selectClause = finalVertexAlias + "." + idCol;
        } else if (!parsed.projections().isEmpty()) {
            StringJoiner projectionSelect = new StringJoiner(", ");
            for (ProjectionField projection : parsed.projections()) {
                if (projection.kind() != ProjectionKind.EDGE_PROPERTY) {
                    throw new IllegalArgumentException("both() projection currently supports by('property') only");
                }
                String mappedColumn = mapVertexProperty(startVertexMapping, projection.property());
                projectionSelect.add(finalVertexAlias + "." + mappedColumn + " AS " + quoteAlias(projection.alias()));
            }
            selectClause = projectionSelect.toString();
        } else if (parsed.pathSeen() && !parsed.pathByProperties().isEmpty()) {
            // path().by('p0').by('p1')... — delegate to the shared helper for per-hop mapping
            selectClause = buildPathSelectClause(
                    parsed.pathByProperties(), hopCount, mappingConfig, startVertexMapping, parsed.hops())
                    .substring("SELECT ".length()); // strip leading "SELECT " since we prepend it below
        } else if (parsed.valueProperty() != null) {
            String mappedColumn = mapVertexProperty(startVertexMapping, parsed.valueProperty());
            selectClause = finalVertexAlias + "." + mappedColumn + " AS " + parsed.valueProperty();
        } else {
            selectClause = finalVertexAlias + ".*";
        }

        // Build the FROM+JOIN body twice — once for out direction, once for in direction
        // then UNION them.
        // Each branch carries its own independent parameter list so bindings are exact.
        // Always subquery-wrap v0 when preHopLimit is set so that LIMIT is inside the subquery
        // and cannot appear before any outer WHERE (mirrors the fix in buildHopTraversalSql).
        boolean useStartSubquery = parsed.preHopLimit() != null;

        // Build the shared start-from fragment (subquery or plain table).
        // The params for the start fragment are identical for both branches (same WHERE / LIMIT).
        StringBuilder startFrom = new StringBuilder();
        List<Object> sharedStartParams = new ArrayList<>();
        if (useStartSubquery) {
            StringBuilder startSubquery = new StringBuilder("(SELECT * FROM ").append(startVertexMapping.table());
            appendWhereClauseForVertex(startSubquery, sharedStartParams, parsed.filters(), startVertexMapping);
            if (parsed.whereClause() != null) {
                if (parsed.whereClause().kind() == WhereKind.EDGE_EXISTS) {
                    appendEdgeExistsPredicate(startSubquery, sharedStartParams, parsed.whereClause(), mappingConfig, startVertexMapping, null, !parsed.filters().isEmpty());
                } else if (parsed.whereClause().kind() == WhereKind.OUT_NEIGHBOR_HAS || parsed.whereClause().kind() == WhereKind.IN_NEIGHBOR_HAS) {
                    appendNeighborHasPredicate(startSubquery, sharedStartParams, parsed.whereClause(), mappingConfig, startVertexMapping, null, !parsed.filters().isEmpty());
                }
            }
            startSubquery.append(" LIMIT ").append(parsed.preHopLimit()).append(")");
            startFrom.append(startSubquery).append(" v0");
        } else {
            startFrom.append(startVertexMapping.table()).append(" v0");
        }

        // Build two branch bodies (OUT direction and IN direction) for each both() hop
        // For simplicity, each branch replaces both() with out/in respectively.
        List<Object> outParams = new ArrayList<>();
        List<Object> inParams = new ArrayList<>();
        StringBuilder outBranch = new StringBuilder("SELECT ").append(selectClause)
                .append(" FROM ").append(startFrom);
        StringBuilder inBranch = new StringBuilder("SELECT ").append(selectClause)
                .append(" FROM ").append(startFrom);

        for (int i = 0; i < hopCount; i++) {
            HopStep hop = parsed.hops().get(i);
            EdgeMapping edgeMapping = resolveEdgeMapping(hop.singleLabel(), mappingConfig);
            String edgeAlias = "e" + (i + 1);
            String fromVertexAlias = "v" + i;
            String toVertexAlias = "v" + (i + 1);
            String fromId = fromVertexAlias + "." + idCol;
            String toId = toVertexAlias + "." + idCol;

            if ("both".equals(hop.direction())) {
                // OUT branch: edge goes out from current vertex
                outBranch.append(" JOIN ").append(edgeMapping.table()).append(" ").append(edgeAlias)
                        .append(" ON ").append(edgeAlias).append(".").append(edgeMapping.outColumn()).append(" = ").append(fromId)
                        .append(" JOIN ").append(startVertexMapping.table()).append(" ").append(toVertexAlias)
                        .append(" ON ").append(toId).append(" = ").append(edgeAlias).append(".").append(edgeMapping.inColumn());
                // IN branch: edge comes in to current vertex
                inBranch.append(" JOIN ").append(edgeMapping.table()).append(" ").append(edgeAlias)
                        .append(" ON ").append(edgeAlias).append(".").append(edgeMapping.inColumn()).append(" = ").append(fromId)
                        .append(" JOIN ").append(startVertexMapping.table()).append(" ").append(toVertexAlias)
                        .append(" ON ").append(toId).append(" = ").append(edgeAlias).append(".").append(edgeMapping.outColumn());
            } else {
                // non-both hop: same join in both branches
                String outJoin = " JOIN " + edgeMapping.table() + " " + edgeAlias + " ON " +
                        edgeAlias + "." + ("out".equals(hop.direction()) ? edgeMapping.outColumn() : edgeMapping.inColumn()) +
                        " = " + fromId +
                        " JOIN " + startVertexMapping.table() + " " + toVertexAlias + " ON " + toId + " = " +
                        edgeAlias + "." + ("out".equals(hop.direction()) ? edgeMapping.inColumn() : edgeMapping.outColumn());
                outBranch.append(outJoin);
                inBranch.append(outJoin);
            }
        }

        // Apply where/filter on v0 if not using start subquery.
        // Each branch collects its own params independently.
        if (!useStartSubquery) {
            boolean hasWhere = false;
            if (!parsed.filters().isEmpty()) {
                appendWhereClauseForVertexAlias(outBranch, outParams, parsed.filters(), startVertexMapping, "v0");
                appendWhereClauseForVertexAlias(inBranch, inParams, parsed.filters(), startVertexMapping, "v0");
                hasWhere = true;
            }
            if (parsed.whereClause() != null && parsed.whereClause().kind() == WhereKind.EDGE_EXISTS) {
                appendEdgeExistsPredicate(outBranch, outParams, parsed.whereClause(), mappingConfig, startVertexMapping, "v0", hasWhere);
                appendEdgeExistsPredicate(inBranch, inParams, parsed.whereClause(), mappingConfig, startVertexMapping, "v0", hasWhere);
            }
        }

        // simplePath() cycle-detection on both branches: each vertex must not equal any prior vertex.
        // v1 != v0, v2 NOT IN (v0,v1), v3 NOT IN (v0,v1,v2), ...
        if (parsed.simplePathRequested() && hopCount > 0) {
            StringJoiner cycleConditions = new StringJoiner(" AND ");
            for (int i = 1; i <= hopCount; i++) {
                String currentAlias = "v" + i + "." + idCol;
                if (i == 1) {
                    cycleConditions.add(currentAlias + " <> v0." + idCol);
                } else {
                    StringJoiner priorAliases = new StringJoiner(", ");
                    for (int j = 0; j < i; j++) {
                        priorAliases.add("v" + j + "." + idCol);
                    }
                    cycleConditions.add(currentAlias + " NOT IN (" + priorAliases + ")");
                }
            }
            String cycleWhere = cycleConditions.toString();
            // Determine whether a WHERE clause was already appended to each branch
            boolean outHasWhere = !useStartSubquery && (!parsed.filters().isEmpty() || parsed.whereClause() != null);
            boolean inHasWhere  = outHasWhere;
            outBranch.append(outHasWhere ? " AND " : " WHERE ").append(cycleWhere);
            inBranch.append(inHasWhere  ? " AND " : " WHERE ").append(cycleWhere);
        }

        // UNION the two branches, wrap with dedup/count
        String unionKeyword = parsed.dedupRequested() ? " UNION " : " UNION ALL ";
        String unionSql = outBranch + unionKeyword + inBranch;

        StringBuilder finalSql;
        if (parsed.countRequested()) {
            if (parsed.dedupRequested()) {
                finalSql = new StringBuilder("SELECT COUNT(DISTINCT ").append(idCol).append(") AS count FROM (").append(unionSql).append(") _u");
            } else {
                finalSql = new StringBuilder("SELECT COUNT(*) AS count FROM (").append(unionSql).append(") _u");
            }
        } else if (parsed.dedupRequested()) {
            finalSql = new StringBuilder("SELECT DISTINCT * FROM (").append(unionSql).append(") _u");
        } else {
            finalSql = new StringBuilder(unionSql);
        }

        // Combine params: sharedStartParams appear once per branch (subquery case),
        // plus each branch's own inline WHERE params.
        List<Object> params = new ArrayList<>();
        if (useStartSubquery) {
            // The subquery is inlined in the SQL text of both branches, so its params appear twice.
            params.addAll(sharedStartParams);
            params.addAll(sharedStartParams);
        } else {
            params.addAll(outParams);
            params.addAll(inParams);
        }

        appendLimit(finalSql, parsed.limit());
        return new TranslationResult(finalSql.toString(), params);
    }

    private TranslationResult buildEdgeSql(ParsedTraversal parsed, MappingConfig mappingConfig) {
        var ref = new Object() {
            String label = parsed.label();
        };
        if (ref.label == null) {
            // Auto-resolve: find the edge label whose mapping contains all has() filter properties
            ref.label = resolveEdgeLabelFromFilters(parsed.filters(), mappingConfig);
        }

        EdgeMapping edgeMapping = Optional.ofNullable(mappingConfig.edges().get(ref.label))
                .orElseThrow(() -> new IllegalArgumentException("No edge mapping found for label: " + ref.label));

        if (!parsed.selectFields().isEmpty()) {
            return buildEdgeAliasSelectSql(parsed, mappingConfig, edgeMapping);
        }

        if (parsed.groupCountProperty() != null) {
            return buildEdgeGroupCountSql(parsed, edgeMapping);
        }

        if (!parsed.projections().isEmpty()) {
            return buildEdgeProjectionSql(parsed, mappingConfig, edgeMapping);
        }

        // outV/inV hop: g.E().has(...).outV() → SELECT v.* FROM vertices v JOIN edges e ON e.out_id = v.id WHERE ...
        if (!parsed.hops().isEmpty()) {
            return buildEdgeToVertexSql(parsed, mappingConfig, edgeMapping);
        }

        String selectClause = parsed.countRequested() ? "COUNT(*) AS count" : "*";
        List<Object> params = new ArrayList<>();
        if (parsed.valueProperty() != null && !parsed.countRequested()) {
            String mappedColumn = mapEdgeProperty(edgeMapping, parsed.valueProperty());
            selectClause = mappedColumn + " AS " + parsed.valueProperty();
        }

        StringBuilder sql = new StringBuilder("SELECT ").append(selectClause)
                .append(" FROM ").append(edgeMapping.table());

        appendWhereClauseForEdge(sql, params, parsed.filters(), edgeMapping);
        appendOrderBy(sql, parsed.orderByProperty(), parsed.orderDirection());
        appendLimit(sql, parsed.limit() != null ? parsed.limit() : parsed.preHopLimit());

        return new TranslationResult(sql.toString(), params);
    }

    private TranslationResult buildEdgeToVertexSql(ParsedTraversal parsed,
                                                    MappingConfig mappingConfig,
                                                    EdgeMapping edgeMapping) {
        // Supports g.E().has(...).outV() and g.E().has(...).inV()
        // Only single outV/inV hop is supported
        if (parsed.hops().size() != 1) {
            throw new IllegalArgumentException("Only a single outV() or inV() hop is supported after g.E()");
        }
        HopStep hop = parsed.hops().get(0);
        if (!"outV".equals(hop.direction()) && !"inV".equals(hop.direction())) {
            throw new IllegalArgumentException("Only outV() or inV() hops are supported after g.E()");
        }
        VertexMapping vertexMapping = Optional.ofNullable(resolveVertexMappingForEdgeProjection(mappingConfig))
                .orElseThrow(() -> new IllegalArgumentException(
                        "outV()/inV() traversal requires exactly one vertex mapping to be present"
                ));

        String joinColumn = "outV".equals(hop.direction()) ? edgeMapping.outColumn() : edgeMapping.inColumn();

        List<Object> params = new ArrayList<>();
        StringBuilder sql = new StringBuilder("SELECT DISTINCT v.*")
                .append(" FROM ").append(vertexMapping.table()).append(" v")
                .append(" JOIN ").append(edgeMapping.table()).append(" e")
                .append(" ON v.").append(vertexMapping.idColumn()).append(" = e.").append(joinColumn);

        appendWhereClauseForEdgeAlias(sql, params, parsed.filters(), edgeMapping, "e");
        appendOrderBy(sql, parsed.orderByProperty(), parsed.orderDirection());
        appendLimit(sql, parsed.limit() != null ? parsed.limit() : parsed.preHopLimit());
        return new TranslationResult(sql.toString(), params);
    }

    /**
     * When no hasLabel() is given on g.E(), auto-resolve the edge label by finding
     * the edge mapping that contains all of the has() filter property keys.
     * If exactly one edge label matches, use it.  If multiple match, falls back to
     * resolveSingleLabel() (throws if > 1 total label).
     */
    private String resolveEdgeLabelFromFilters(List<HasFilter> filters, MappingConfig mappingConfig) {
        if (mappingConfig.edges().size() == 1) {
            return mappingConfig.edges().keySet().iterator().next();
        }
        if (filters.isEmpty()) {
            return resolveSingleLabel(mappingConfig.edges(), "edge");
        }
        List<String> candidates = new ArrayList<>();
        for (Map.Entry<String, EdgeMapping> entry : mappingConfig.edges().entrySet()) {
            EdgeMapping em = entry.getValue();
            boolean allMatch = filters.stream().allMatch(f ->
                    "id".equals(f.property())
                    || "outV".equals(f.property())
                    || "inV".equals(f.property())
                    || em.properties().containsKey(f.property()));
            if (allMatch) {
                candidates.add(entry.getKey());
            }
        }
        if (candidates.size() == 1) {
            return candidates.get(0);
        }
        return resolveSingleLabel(mappingConfig.edges(), "edge");
    }

    private TranslationResult buildEdgeAliasSelectSql(ParsedTraversal parsed,
                                                      MappingConfig mappingConfig,
                                                      EdgeMapping rootEdgeMapping) {
        if (parsed.countRequested()) {
            throw new IllegalArgumentException("count() cannot be combined with select(...)");
        }
        if (parsed.valueProperty() != null) {
            throw new IllegalArgumentException("values() cannot be combined with select(...)");
        }
        if (parsed.whereClause() != null && parsed.whereClause().kind() != WhereKind.NEQ_ALIAS) {
            throw new IllegalArgumentException("Only where('a',neq('b')) is supported with select(...)");
        }

        // Supported path: g.E()...outV().as('a').outE(...).inV().as('b').where('a',neq('b')).select('a','b').by('prop')
        if (parsed.hops().size() != 3
                || !"outV".equals(parsed.hops().get(0).direction())
                || !("outE".equals(parsed.hops().get(1).direction()) || "inE".equals(parsed.hops().get(1).direction()))
                || !("inV".equals(parsed.hops().get(2).direction()) || "outV".equals(parsed.hops().get(2).direction()))) {
            throw new IllegalArgumentException("select(...).by(...) from g.E() currently supports outV().outE()/inE().inV()/outV() pattern");
        }

        VertexMapping vertexMapping = Optional.ofNullable(resolveVertexMappingForEdgeProjection(mappingConfig))
                .orElseThrow(() -> new IllegalArgumentException("select(...).by('prop') from g.E() requires exactly one vertex mapping"));

        // Map alias label -> hop index position captured at as(...)
        Map<String, Integer> aliasHopIndex = new java.util.HashMap<>();
        for (AsAlias asAlias : parsed.asAliases()) {
            aliasHopIndex.put(asAlias.label(), asAlias.hopIndexAfter());
        }

        String edge2Label = parsed.hops().get(1).singleLabel();
        EdgeMapping secondEdgeMapping = resolveEdgeMapping(edge2Label, mappingConfig);

        List<Object> params = new ArrayList<>();
        StringJoiner selectJoiner = new StringJoiner(", ");
        for (SelectField field : parsed.selectFields()) {
            Integer hopIndex = aliasHopIndex.get(field.alias);
            if (hopIndex == null) {
                throw new IllegalArgumentException("select alias not found: " + field.alias);
            }
            if (field.property() == null || field.property().isBlank()) {
                throw new IllegalArgumentException("select(...).by(...) requires a property for alias: " + field.alias);
            }
            String vertexAlias = "v" + hopIndex;
            String column = mapVertexProperty(vertexMapping, field.property());
            selectJoiner.add(vertexAlias + "." + column + " AS " + quoteAlias(field.alias));
        }

        StringBuilder sql = new StringBuilder("SELECT DISTINCT ")
                .append(selectJoiner)
                .append(" FROM ").append(rootEdgeMapping.table()).append(" e0")
                .append(" JOIN ").append(vertexMapping.table()).append(" v1")
                .append(" ON v1.").append(vertexMapping.idColumn()).append(" = e0.")
                .append(rootEdgeMapping.outColumn())
                .append(" JOIN ").append(secondEdgeMapping.table()).append(" e2");

        if ("outE".equals(parsed.hops().get(1).direction())) {
            sql.append(" ON e2.").append(secondEdgeMapping.outColumn()).append(" = v1.").append(vertexMapping.idColumn());
        } else {
            sql.append(" ON e2.").append(secondEdgeMapping.inColumn()).append(" = v1.").append(vertexMapping.idColumn());
        }

        sql.append(" JOIN ").append(vertexMapping.table()).append(" v3");
        if ("inV".equals(parsed.hops().get(2).direction())) {
            sql.append(" ON v3.").append(vertexMapping.idColumn()).append(" = e2.").append(secondEdgeMapping.inColumn());
        } else {
            sql.append(" ON v3.").append(vertexMapping.idColumn()).append(" = e2.").append(secondEdgeMapping.outColumn());
        }

        StringJoiner whereJoiner = new StringJoiner(" AND ");
        List<HasFilter> edgeFilters = parsed.filters();
        if (!edgeFilters.isEmpty()) {
            HasFilter first = edgeFilters.get(0);
            whereJoiner.add("e0." + mapEdgeFilterProperty(rootEdgeMapping, first.property()) + " = ?");
            params.add(first.value());
            for (int i = 1; i < edgeFilters.size(); i++) {
                HasFilter filter = edgeFilters.get(i);
                whereJoiner.add("e2." + mapEdgeFilterProperty(secondEdgeMapping, filter.property()) + " = ?");
                params.add(filter.value());
            }
        }

        if (parsed.whereClause() != null) {
            String leftVertexAlias = resolveVertexAliasForWhere(parsed.whereClause().left(), aliasHopIndex);
            String rightVertexAlias = resolveVertexAliasForWhere(parsed.whereClause().right(), aliasHopIndex);
            whereJoiner.add(leftVertexAlias + "." + vertexMapping.idColumn() + " <> " + rightVertexAlias + "." + vertexMapping.idColumn());
        }

        String whereSql = whereJoiner.toString();
        if (!whereSql.isBlank()) {
            sql.append(" WHERE ").append(whereSql);
        }
        appendOrderBy(sql, parsed.orderByProperty(), parsed.orderDirection());
        appendLimit(sql, parsed.limit() != null ? parsed.limit() : parsed.preHopLimit());
        return new TranslationResult(sql.toString(), params);
    }

    private TranslationResult buildEdgeProjectionSql(ParsedTraversal parsed,
                                                     MappingConfig mappingConfig,
                                                     EdgeMapping edgeMapping) {
        if (parsed.countRequested()) {
            throw new IllegalArgumentException("count() cannot be combined with project(...)");
        }
        if (parsed.valueProperty() != null) {
            throw new IllegalArgumentException("values() cannot be combined with project(...)");
        }

        List<Object> params = new ArrayList<>();
        StringJoiner selectJoiner = new StringJoiner(", ");
        boolean needsOutJoin = false;
        boolean needsInJoin = false;

        // Resolve vertex mappings for outV and inV sides of this edge.
        // Strategy 1: use edge-column-name heuristic (works for semantic names like "account_id").
        // Strategy 2: when that returns null, scan which vertex mappings contain the requested properties.
        // Strategy 3: fall back to the single-vertex-mapping shortcut.
        VertexMapping outVertexMapping = resolveHopTargetVertexMapping(mappingConfig, edgeMapping, false, null);
        VertexMapping inVertexMapping  = resolveHopTargetVertexMapping(mappingConfig, edgeMapping, true,  null);

        if (outVertexMapping == null || inVertexMapping == null) {
            // Collect requested property names per side
            List<String> outProps = new ArrayList<>();
            List<String> inProps  = new ArrayList<>();
            for (ProjectionField pf : parsed.projections()) {
                if (pf.kind() == ProjectionKind.OUT_VERTEX_PROPERTY) outProps.add(pf.property());
                if (pf.kind() == ProjectionKind.IN_VERTEX_PROPERTY)  inProps.add(pf.property());
            }
            VertexMapping fallback = resolveVertexMappingForEdgeProjection(mappingConfig);

            if (outVertexMapping == null) {
                outVertexMapping = resolveVertexMappingByProperties(mappingConfig, outProps);
                if (outVertexMapping == null) outVertexMapping = fallback;
            }
            if (inVertexMapping == null) {
                inVertexMapping = resolveVertexMappingByProperties(mappingConfig, inProps);
                if (inVertexMapping == null) inVertexMapping = fallback;
            }

            if (outVertexMapping == null || inVertexMapping == null) {
                throw new IllegalArgumentException(
                        "project(...).by(outV()/inV()) requires exactly one vertex mapping or a resolvable vertex label");
            }
        }

        for (ProjectionField projection : parsed.projections()) {
            String alias = quoteAlias(projection.alias());
            if (projection.kind() == ProjectionKind.EDGE_PROPERTY) {
                String column = mapEdgeProperty(edgeMapping, projection.property());
                selectJoiner.add("e." + column + " AS " + alias);
            } else if (projection.kind() == ProjectionKind.OUT_VERTEX_PROPERTY) {
                String column = mapVertexProperty(outVertexMapping, projection.property());
                selectJoiner.add("ov." + column + " AS " + alias);
                needsOutJoin = true;
            } else if (projection.kind() == ProjectionKind.IN_VERTEX_PROPERTY) {
                String column = mapVertexProperty(inVertexMapping, projection.property());
                selectJoiner.add("iv." + column + " AS " + alias);
                needsInJoin = true;
            }
        }

        StringBuilder sql = new StringBuilder("SELECT ")
                .append(selectJoiner)
                .append(" FROM ").append(edgeMapping.table()).append(" e");

        if (needsOutJoin) {
            sql.append(" JOIN ").append(outVertexMapping.table()).append(" ov")
                    .append(" ON ov.").append(outVertexMapping.idColumn()).append(" = e.").append(edgeMapping.outColumn());
        }
        if (needsInJoin) {
            sql.append(" JOIN ").append(inVertexMapping.table()).append(" iv")
                    .append(" ON iv.").append(inVertexMapping.idColumn()).append(" = e.").append(edgeMapping.inColumn());
        }

        appendWhereClauseForEdgeAlias(sql, params, parsed.filters(), edgeMapping, "e");
        appendLimit(sql, parsed.limit() != null ? parsed.limit() : parsed.preHopLimit());
        return new TranslationResult(sql.toString(), params);
    }

    private TranslationResult buildVertexProjectionSql(ParsedTraversal parsed,
                                                       MappingConfig mappingConfig,
                                                       VertexMapping vertexMapping) {
        if (parsed.countRequested()) {
            throw new IllegalArgumentException("count() cannot be combined with project(...)");
        }
        if (parsed.valueProperty() != null) {
            throw new IllegalArgumentException("values() cannot be combined with project(...)");
        }

        List<Object> params = new ArrayList<>();
        StringJoiner selectJoiner = new StringJoiner(", ");
        int neighborJoinIdx = 0;

        for (ProjectionField projection : parsed.projections()) {
            String alias = quoteAlias(projection.alias());
            if (projection.kind() == ProjectionKind.EDGE_PROPERTY) {
                String column = mapVertexProperty(vertexMapping, projection.property());
                selectJoiner.add("v." + column + " AS " + alias);
            } else if (projection.kind() == ProjectionKind.EDGE_DEGREE) {
                // Encoded property: direction:edgeLabel[:prop=val:prop2=val2...]
                String[] parts = projection.property().split(":", -1);
                if (parts.length < 2) {
                    throw new IllegalArgumentException("Invalid EDGE_DEGREE property: " + projection.property());
                }
                String direction = parts[0];
                String edgeLabel = parts[1];
                EdgeMapping edgeMapping = Optional.ofNullable(mappingConfig.edges().get(edgeLabel))
                        .orElseThrow(() -> new IllegalArgumentException("No edge mapping found for label: " + edgeLabel));
                String joinColumn = "out".equals(direction) ? edgeMapping.outColumn() : edgeMapping.inColumn();
                StringBuilder subquery = new StringBuilder("(SELECT COUNT(*) FROM ")
                        .append(edgeMapping.table())
                        .append(" WHERE ").append(joinColumn).append(" = v.").append(vertexMapping.idColumn());
                for (int i = 2; i < parts.length; i++) {
                    int eq = parts[i].indexOf('=');
                    if (eq < 1) continue;
                    String filterProp = parts[i].substring(0, eq);
                    String filterVal = parts[i].substring(eq + 1);
                    String filterCol = mapEdgeProperty(edgeMapping, filterProp);
                    subquery.append(" AND ").append(filterCol).append(" = '").append(filterVal.replace("'", "''")).append("'");
                }
                subquery.append(")");
                selectJoiner.add(subquery + " AS " + alias);
            } else if (projection.kind() == ProjectionKind.OUT_VERTEX_COUNT
                    || projection.kind() == ProjectionKind.IN_VERTEX_COUNT) {
                // Encoded property: "direction:edgeLabel[:prop=val...]"
                // Translates to: (SELECT COUNT(*) FROM <edge_table> e
                //                   JOIN <target_vertex_table> tv ON tv.<id> = e.<target_col>
                //                  WHERE e.<anchor_col> = v.<id>
                //                    AND tv.<mapped_col> = 'val' ...)
                String[] parts = projection.property().split(":", -1);
                if (parts.length < 2) {
                    throw new IllegalArgumentException("Invalid VERTEX_COUNT property: " + projection.property());
                }
                String direction = parts[0]; // "out" or "in"
                String edgeLabel = parts[1];
                EdgeMapping edgeMapping = Optional.ofNullable(mappingConfig.edges().get(edgeLabel))
                        .orElseThrow(() -> new IllegalArgumentException("No edge mapping found for label: " + edgeLabel));
                String anchorCol = "out".equals(direction) ? edgeMapping.outColumn() : edgeMapping.inColumn();
                String targetCol = "out".equals(direction) ? edgeMapping.inColumn()  : edgeMapping.outColumn();
                VertexMapping targetVertexMapping = resolveTargetVertexMapping(mappingConfig, edgeMapping, "out".equals(direction));
                if (targetVertexMapping == null) {
                    StringBuilder subq = new StringBuilder("(SELECT COUNT(*) FROM ")
                            .append(edgeMapping.table())
                            .append(" WHERE ").append(anchorCol).append(" = v.").append(vertexMapping.idColumn())
                            .append(")");
                    selectJoiner.add(subq + " AS " + alias);
                } else {
                    StringBuilder subq = new StringBuilder("(SELECT COUNT(*) FROM ")
                            .append(edgeMapping.table()).append(" _e")
                            .append(" JOIN ").append(targetVertexMapping.table()).append(" _tv")
                            .append(" ON _tv.").append(targetVertexMapping.idColumn())
                            .append(" = _e.").append(targetCol)
                            .append(" WHERE _e.").append(anchorCol)
                            .append(" = v.").append(vertexMapping.idColumn());
                    for (int i = 2; i < parts.length; i++) {
                        int eq = parts[i].indexOf('=');
                        if (eq < 1) continue;
                        String filterProp = parts[i].substring(0, eq);
                        String filterVal  = parts[i].substring(eq + 1);
                        String filterCol  = mapVertexProperty(targetVertexMapping, filterProp);
                        subq.append(" AND _tv.").append(filterCol)
                            .append(" = '").append(filterVal.replace("'", "''")).append("'");
                    }
                    subq.append(")");
                    selectJoiner.add(subq + " AS " + alias);
                }
            } else if (projection.kind() == ProjectionKind.OUT_NEIGHBOR_PROPERTY
                    || projection.kind() == ProjectionKind.IN_NEIGHBOR_PROPERTY) {
                // Encoded property: "direction:edgeLabel:prop"
                // Translates to a correlated STRING_AGG subquery so that each anchor row
                // receives a single aggregated value, matching Gremlin's fold() semantics
                // and avoiding the duplicate-row problem that a LEFT JOIN would introduce.
                //
                //   (SELECT STRING_AGG(_njv.col, ',')
                //      FROM <edge_table> _nje
                //      JOIN <neighbor_table> _njv ON _njv.<id> = _nje.<target_col>
                //     WHERE _nje.<anchor_col> = v.<id>)  AS "alias"
                String[] parts = projection.property().split(":", 3);
                if (parts.length < 3) {
                    throw new IllegalArgumentException("Invalid NEIGHBOR_PROPERTY descriptor: " + projection.property());
                }
                boolean isOut = "out".equals(parts[0]);
                String edgeLabel    = parts[1];
                String neighborProp = parts[2];
                EdgeMapping edgeMapping = Optional.ofNullable(mappingConfig.edges().get(edgeLabel))
                        .orElseThrow(() -> new IllegalArgumentException("No edge mapping found for label: " + edgeLabel));
                VertexMapping neighborMapping = resolveTargetVertexMapping(mappingConfig, edgeMapping, isOut);
                if (neighborMapping == null) neighborMapping = vertexMapping; // fallback

                String anchorCol = isOut ? edgeMapping.outColumn() : edgeMapping.inColumn();
                String targetCol = isOut ? edgeMapping.inColumn()  : edgeMapping.outColumn();
                String mappedCol = mapVertexProperty(neighborMapping, neighborProp);

                String subqAlias = "_nje" + neighborJoinIdx;
                String vAlias    = "_njv" + neighborJoinIdx;
                neighborJoinIdx++;

                String subq = "(SELECT STRING_AGG(" + vAlias + "." + mappedCol + ", ',')" +
                        " FROM " + edgeMapping.table() + " " + subqAlias +
                        " JOIN " + neighborMapping.table() + " " + vAlias +
                        " ON " + vAlias + "." + neighborMapping.idColumn() +
                        " = " + subqAlias + "." + targetCol +
                        " WHERE " + subqAlias + "." + anchorCol + " = v." + vertexMapping.idColumn() + ")";
                selectJoiner.add(subq + " AS " + alias);
            }
        }  // end for projections

        StringBuilder baseSql = new StringBuilder(parsed.dedupRequested() ? "SELECT DISTINCT " : "SELECT ")
                .append(selectJoiner)
                .append(" FROM ").append(vertexMapping.table()).append(" v");


        appendWhereClauseForVertexAlias(baseSql, params, parsed.filters(), vertexMapping, "v");
        if (parsed.whereClause() != null && parsed.whereClause().kind() == WhereKind.EDGE_EXISTS) {
            appendEdgeExistsPredicate(baseSql, params, parsed.whereClause(), mappingConfig, vertexMapping, "v", !parsed.filters().isEmpty());
        } else if (parsed.whereClause() != null && (parsed.whereClause().kind() == WhereKind.OUT_NEIGHBOR_HAS || parsed.whereClause().kind() == WhereKind.IN_NEIGHBOR_HAS)) {
            appendNeighborHasPredicate(baseSql, params, parsed.whereClause(), mappingConfig, vertexMapping, "v", !parsed.filters().isEmpty());
        }

        if (parsed.whereClause() != null) {
            if (parsed.whereClause().kind() != WhereKind.PROJECT_GT && parsed.whereClause().kind() != WhereKind.PROJECT_GTE) {
                if (parsed.whereClause().kind() == WhereKind.EDGE_EXISTS
                        || parsed.whereClause().kind() == WhereKind.OUT_NEIGHBOR_HAS
                        || parsed.whereClause().kind() == WhereKind.IN_NEIGHBOR_HAS) {
                    appendOrderBy(baseSql, parsed.orderByProperty(), parsed.orderDirection());
                    Integer effectiveLimit = parsed.limit() != null ? parsed.limit() : parsed.preHopLimit();
                    appendLimit(baseSql, effectiveLimit);
                    return new TranslationResult(baseSql.toString(), params);
                }
                throw new IllegalArgumentException("Only where(select('alias').is(gt/gte(n))) or where(outE/inE/bothE(...)) or where(out/in('label').has(...)) is supported with project(...)");
            }
            String numericLiteral = sanitizeNumericLiteral(parsed.whereClause().right());
            String operator = parsed.whereClause().kind() == WhereKind.PROJECT_GTE ? ">=" : ">";
            StringBuilder wrapped = new StringBuilder("SELECT * FROM (")
                    .append(baseSql)
                    .append(") p WHERE p.")
                    .append(quoteAlias(parsed.whereClause().left()))
                    .append(" ").append(operator).append(" ").append(numericLiteral);
            Integer effectiveLimit = parsed.limit() != null ? parsed.limit() : parsed.preHopLimit();
            appendOrderBy(wrapped, parsed.orderByProperty(), parsed.orderDirection());
            appendLimit(wrapped, effectiveLimit);
            return new TranslationResult(wrapped.toString(), params);
        }

        Integer effectiveLimit = parsed.limit() != null ? parsed.limit() : parsed.preHopLimit();
        appendOrderBy(baseSql, parsed.orderByProperty(), parsed.orderDirection());
        appendLimit(baseSql, effectiveLimit);

        return new TranslationResult(baseSql.toString(), params);
    }

    private String mapEdgeFilterProperty(EdgeMapping mapping, String property) {
        if ("id".equals(property)) {
            return mapping.idColumn();
        }
        if ("outV".equals(property)) {
            return mapping.outColumn();
        }
        if ("inV".equals(property)) {
            return mapping.inColumn();
        }
        return mapEdgeProperty(mapping, property);
    }

    private String resolveVertexAliasForWhere(String aliasLabel, Map<String, Integer> aliasHopIndex) {
        Integer hopIndex = aliasHopIndex.get(aliasLabel);
        if (hopIndex == null) {
            throw new IllegalArgumentException("where() alias not found: " + aliasLabel);
        }
        return "v" + hopIndex;
    }

    private String sanitizeNumericLiteral(String raw) {
        String value = raw == null ? "" : raw.trim();
        if (!value.matches("-?\\d+(\\.\\d+)?")) {
            throw new IllegalArgumentException("where(...).is(gt/gte(...)) expects a numeric literal");
        }
        return value;
    }

    private void appendEdgeExistsPredicate(StringBuilder sql,
                                           List<Object> params,
                                           WhereClause whereClause,
                                           MappingConfig mappingConfig,
                                           VertexMapping vertexMapping,
                                           String vertexAlias,
                                           boolean hasExistingWhere) {
        EdgeMapping edgeMapping = resolveEdgeMapping(whereClause.right(), mappingConfig);
        String edgeAlias = "we";
        String idRef = (vertexAlias == null ? vertexMapping.idColumn() : vertexAlias + "." + vertexMapping.idColumn());
        String correlation;
        if ("outE".equals(whereClause.left())) {
            correlation = edgeAlias + "." + edgeMapping.outColumn() + " = " + idRef;
        } else if ("inE".equals(whereClause.left())) {
            correlation = edgeAlias + "." + edgeMapping.inColumn() + " = " + idRef;
        } else {
            correlation = "(" + edgeAlias + "." + edgeMapping.outColumn() + " = " + idRef
                    + " OR " + edgeAlias + "." + edgeMapping.inColumn() + " = " + idRef + ")";
        }

        StringBuilder existsSql = new StringBuilder("EXISTS (SELECT 1 FROM ")
                .append(edgeMapping.table()).append(' ').append(edgeAlias)
                .append(" WHERE ").append(correlation);
        for (HasFilter filter : whereClause.filters()) {
            String column = mapEdgeFilterProperty(edgeMapping, filter.property());
            existsSql.append(" AND ").append(edgeAlias).append('.').append(column).append(" = ?");
            params.add(filter.value());
        }
        existsSql.append(")");

        sql.append(hasExistingWhere ? " AND " : " WHERE ").append(existsSql);
    }

    /**
     * Appends a WHERE predicate for {@code where(out('label').has('prop','val'))} and
     * {@code where(in('label').has('prop','val'))}.
     * <p>
     * Translates to:
     * <pre>
     * EXISTS (SELECT 1 FROM &lt;edge_table&gt; _we
     *           JOIN &lt;vertex_table&gt; _wv ON _wv.&lt;id&gt; = _we.&lt;target_col&gt;
     *          WHERE _we.&lt;anchor_col&gt; = &lt;anchor_id_ref&gt;
     *            AND _wv.&lt;mapped_prop_col&gt; = ?)
     * </pre>
     * For {@code out('label')}: the anchor vertex is the out-vertex of the edge, and the
     * neighbor (filter target) is the in-vertex.
     * For {@code in('label')}: the anchor vertex is the in-vertex, neighbor is the out-vertex.
     *
     * @param sql               the SQL builder to append to
     * @param params            parameter list to accumulate bind values into
     * @param whereClause       the parsed where clause (left=direction, right=edgeLabel, filters=vertex property filters)
     * @param mappingConfig     full mapping configuration
     * @param vertexMapping     mapping for the anchor vertex (the one being filtered)
     * @param vertexAlias       optional table alias for the anchor vertex (null → unqualified)
     * @param hasExistingWhere  true if a WHERE clause is already present (appends AND, otherwise WHERE)
     */
    private void appendNeighborHasPredicate(StringBuilder sql,
                                            List<Object> params,
                                            WhereClause whereClause,
                                            MappingConfig mappingConfig,
                                            VertexMapping vertexMapping,
                                            String vertexAlias,
                                            boolean hasExistingWhere) {
        // whereClause.right() is the edge label; whereClause.left() is "out" or "in"
        EdgeMapping edgeMapping = resolveEdgeMapping(whereClause.right(), mappingConfig);

        // Anchor column: the edge column that references the starting vertex
        // Target column: the edge column that references the neighbor vertex
        boolean isOut = "out".equals(whereClause.left());
        String anchorCol = isOut ? edgeMapping.outColumn() : edgeMapping.inColumn();
        String targetCol = isOut ? edgeMapping.inColumn()  : edgeMapping.outColumn();

        // The neighbor vertex mapping (for property resolution)
        VertexMapping neighborVertexMapping = resolveTargetVertexMapping(mappingConfig, edgeMapping, isOut);
        if (neighborVertexMapping == null) {
            // Fallback: no vertex table available — we can only correlate on the edge itself
            neighborVertexMapping = vertexMapping;
        }

        String idRef = (vertexAlias == null
                ? vertexMapping.idColumn()
                : vertexAlias + "." + vertexMapping.idColumn());

        StringBuilder existsSql = new StringBuilder("EXISTS (SELECT 1 FROM ")
                .append(edgeMapping.table()).append(" _we")
                .append(" JOIN ").append(neighborVertexMapping.table()).append(" _wv")
                .append(" ON _wv.").append(neighborVertexMapping.idColumn())
                .append(" = _we.").append(targetCol)
                .append(" WHERE _we.").append(anchorCol).append(" = ").append(idRef);

        for (HasFilter filter : whereClause.filters()) {
            String column = mapVertexProperty(neighborVertexMapping, filter.property());
            existsSql.append(" AND _wv.").append(column).append(" = ?");
            params.add(filter.value());
        }
        existsSql.append(")");

        sql.append(hasExistingWhere ? " AND " : " WHERE ").append(existsSql);
    }

    private void appendWhereClauseForVertex(StringBuilder sql, List<Object> params, List<HasFilter> filters, VertexMapping mapping) {
        appendWhereClauseForVertexAlias(sql, params, filters, mapping, null);
    }

    private void appendWhereClauseForVertexAlias(StringBuilder sql,
                                                 List<Object> params,
                                                 List<HasFilter> filters,
                                                 VertexMapping mapping,
                                                 String alias) {
        if (filters.isEmpty()) {
            return;
        }
        StringJoiner whereJoiner = new StringJoiner(" AND ");
        for (HasFilter filter : filters) {
            String column = "id".equals(filter.property()) ? mapping.idColumn() : mapVertexProperty(mapping, filter.property());
            if (alias != null) {
                whereJoiner.add(alias + "." + column + " = ?");
            } else {
                whereJoiner.add(column + " = ?");
            }
            params.add(filter.value());
        }
        sql.append(" WHERE ").append(whereJoiner);
    }

    private void appendWhereClauseForEdge(StringBuilder sql, List<Object> params, List<HasFilter> filters, EdgeMapping mapping) {
        appendWhereClauseForEdgeAlias(sql, params, filters, mapping, null);
    }

    private void appendWhereClauseForEdgeAlias(StringBuilder sql,
                                               List<Object> params,
                                               List<HasFilter> filters,
                                               EdgeMapping mapping,
                                               String alias) {
        if (filters.isEmpty()) {
            return;
        }
        StringJoiner whereJoiner = new StringJoiner(" AND ");
        for (HasFilter filter : filters) {
            String column;
            if ("id".equals(filter.property())) {
                column = mapping.idColumn();
            } else if ("outV".equals(filter.property())) {
                column = mapping.outColumn();
            } else if ("inV".equals(filter.property())) {
                column = mapping.inColumn();
            } else {
                column = mapEdgeProperty(mapping, filter.property());
            }
            String qualifiedColumn = alias == null ? column : alias + "." + column;
            whereJoiner.add(qualifiedColumn + " = ?");
            params.add(filter.value());
        }
        sql.append(" WHERE ").append(whereJoiner);
    }

    private static void appendLimit(StringBuilder sql, Integer limit) {
        if (limit != null) {
            sql.append(" LIMIT ").append(limit);
        }
    }

    private static void appendOrderBy(StringBuilder sql, String orderByProperty, String orderDirection) {
        if (orderByProperty != null) {
            String direction = orderDirection != null ? orderDirection : "ASC";
            sql.append(" ORDER BY \"").append(orderByProperty).append("\" ").append(direction);
        }
    }

    private String mapVertexProperty(VertexMapping mapping, String property) {
        String column = mapping.properties().get(property);
        if (column == null) {
            throw new IllegalArgumentException("No vertex property mapping found for property: " + property);
        }
        return column;
    }

    private String mapEdgeProperty(EdgeMapping mapping, String property) {
        String column = mapping.properties().get(property);
        if (column == null) {
            throw new IllegalArgumentException("No edge property mapping found for property: " + property);
        }
        return column;
    }

    private String resolveSingleLabel(Map<String, ?> map, String kind) {
        if (map.size() != 1) {
            throw new IllegalArgumentException("Traversal must include hasLabel() when more than one " + kind + " label is mapped");
        }
        return map.keySet().iterator().next();
    }

    private EdgeMapping resolveEdgeMapping(String edgeLabelOrNull, MappingConfig mappingConfig) {
        String resolvedEdgeLabel = edgeLabelOrNull;
        if (resolvedEdgeLabel == null || resolvedEdgeLabel.isBlank()) {
            resolvedEdgeLabel = resolveSingleLabel(mappingConfig.edges(), "edge");
        }
        String labelForLookup = resolvedEdgeLabel;
        return Optional.ofNullable(mappingConfig.edges().get(labelForLookup))
                .orElseThrow(() -> new IllegalArgumentException("No edge mapping found for label: " + labelForLookup));
    }

    private TranslationResult buildEdgeGroupCountSql(ParsedTraversal parsed,
                                                     EdgeMapping edgeMapping) {
        if (parsed.countRequested()) {
            throw new IllegalArgumentException("count() cannot be combined with groupCount()");
        }
        if (parsed.valueProperty() != null) {
            throw new IllegalArgumentException("values() cannot be combined with groupCount()");
        }
        if (!parsed.projections().isEmpty()) {
            throw new IllegalArgumentException("project(...) cannot be combined with groupCount()");
        }

        String groupProperty = parsed.groupCountProperty();
        String mappedColumn = mapEdgeProperty(edgeMapping, groupProperty);

        List<Object> params = new ArrayList<>();
        StringBuilder sql = new StringBuilder("SELECT ")
                .append(mappedColumn).append(" AS ").append(quoteAlias(groupProperty))
                .append(", COUNT(*) AS count")
                .append(" FROM ").append(edgeMapping.table());

        appendWhereClauseForEdge(sql, params, parsed.filters(), edgeMapping);
        sql.append(" GROUP BY ").append(mappedColumn);
        appendOrderBy(sql, parsed.orderByProperty(), parsed.orderDirection());
        appendLimit(sql, parsed.limit() != null ? parsed.limit() : parsed.preHopLimit());

        return new TranslationResult(sql.toString(), params);
    }

    private TranslationResult buildVertexGroupCountSql(ParsedTraversal parsed,
                                                       VertexMapping vertexMapping) {
        String groupProperty = parsed.groupCountProperty();
        String mappedColumn = mapVertexProperty(vertexMapping, groupProperty);

        List<Object> params = new ArrayList<>();
        StringBuilder sql = new StringBuilder("SELECT ")
                .append(mappedColumn).append(" AS ").append(quoteAlias(groupProperty))
                .append(", COUNT(*) AS count")
                .append(" FROM ").append(vertexMapping.table());

        appendWhereClauseForVertex(sql, params, parsed.filters(), vertexMapping);
        sql.append(" GROUP BY ").append(mappedColumn);
        appendOrderBy(sql, parsed.orderByProperty(), parsed.orderDirection());
        appendLimit(sql, parsed.limit() != null ? parsed.limit() : parsed.preHopLimit());

        return new TranslationResult(sql.toString(), params);
    }

    private ParsedTraversal parseSteps(String query, int startIndex, String rootIdFilter) {
        int index = startIndex;
        String label = null;
        String valuesProperty = null;
        String groupCountProperty = null;
        boolean groupCountSeen = false;
        boolean orderSeen = false;
        String orderByProperty = null;
        String orderDirection = null;
        Integer limit = null;
        Integer preHopLimit = null;
        boolean hopsSeen = false;
        boolean countRequested = false;
        List<HasFilter> filters = new ArrayList<>();
        List<HopStep> hops = new ArrayList<>();
        List<String> projectAliases = new ArrayList<>();
        List<String> byExpressions = new ArrayList<>();
        RepeatHopResult pendingRepeatHop = null;
        // as / select / where support
        List<AsAlias> asAliases = new ArrayList<>();
        List<String> selectAliases = new ArrayList<>();
        List<String> selectByExpressions = new ArrayList<>();
        WhereClause whereClause = null;
        boolean selectSeen = false;
        boolean dedupRequested = false;
        boolean pathSeen = false;
        List<String> pathByProperties = new ArrayList<>();
        boolean simplePathRequested = false;

        if (rootIdFilter != null) {
            filters.add(new HasFilter("id", rootIdFilter));
        }

        while (index < query.length()) {
            if (query.charAt(index) != '.') {
                throw new IllegalArgumentException("Invalid traversal near: " + query.substring(index));
            }
            int nameStart = index + 1;
            int nameEnd = nameStart;
            while (nameEnd < query.length() && Character.isAlphabetic(query.charAt(nameEnd))) {
                nameEnd++;
            }
            if (nameEnd >= query.length() || query.charAt(nameEnd) != '(') {
                throw new IllegalArgumentException("Invalid step near: " + query.substring(index));
            }

            String stepName = query.substring(nameStart, nameEnd);
            ParsedArgs parsedArgs = readArgs(query, nameEnd);
            List<String> args = splitArgs(parsedArgs.args());

            switch (stepName) {
                case "hasLabel" -> {
                    ensureArgCount(stepName, args, 1);
                    label = unquote(args.get(0));
                }
                case "has" -> {
                    ensureArgCount(stepName, args, 2);
                    filters.add(new HasFilter(unquote(args.get(0)), unquote(args.get(1))));
                }
                case "values" -> {
                    ensureArgCount(stepName, args, 1);
                    valuesProperty = unquote(args.get(0));
                }
                case "out" -> {
                    if (args.isEmpty()) {
                        throw new IllegalArgumentException("out expects at least 1 argument");
                    }
                    hopsSeen = true;
                    List<String> labels = args.stream().map(this::unquote).toList();
                    hops.add(new HopStep("out", labels));
                }
                case "in" -> {
                    if (args.isEmpty()) {
                        throw new IllegalArgumentException("in expects at least 1 argument");
                    }
                    hopsSeen = true;
                    List<String> labels = args.stream().map(this::unquote).toList();
                    hops.add(new HopStep("in", labels));
                }
                case "outV" -> {
                    ensureArgCount(stepName, args, 0);
                    hopsSeen = true;
                    hops.add(new HopStep("outV", List.of()));
                }
                case "inV" -> {
                    ensureArgCount(stepName, args, 0);
                    hopsSeen = true;
                    hops.add(new HopStep("inV", List.of()));
                }
                case "both" -> {
                    if (args.size() > 1) {
                        throw new IllegalArgumentException("both expects 0 or 1 argument(s)");
                    }
                    hopsSeen = true;
                    List<String> labels = args.isEmpty() ? List.of() : List.of(unquote(args.get(0)));
                    hops.add(new HopStep("both", labels));
                }
                case "repeat" -> {
                    if (parsedArgs.args().isBlank()) {
                        throw new IllegalArgumentException("repeat() requires an argument");
                    }
                    hopsSeen = true;
                    pendingRepeatHop = parseRepeatHop(parsedArgs.args().trim());
                }
                case "simplePath" -> {
                    ensureArgCount(stepName, args, 0);
                    simplePathRequested = true;
                }
                case "times" -> {
                    if (pendingRepeatHop == null) {
                        throw new IllegalArgumentException("times() must follow repeat(out(...)), repeat(in(...)), or repeat(both(...))");
                    }
                    ensureArgCount(stepName, args, 1);
                    int repeatCount;
                    try {
                        repeatCount = Integer.parseInt(args.get(0).trim());
                    } catch (NumberFormatException nfe) {
                        throw new IllegalArgumentException("times() must be numeric");
                    }
                    if (repeatCount < 0) {
                        throw new IllegalArgumentException("times() must be >= 0");
                    }
                    if (pendingRepeatHop.simplePathDetected()) {
                        simplePathRequested = true;
                    }
                    for (int i = 0; i < repeatCount; i++) {
                        hops.add(pendingRepeatHop.hop());
                    }
                    pendingRepeatHop = null;
                }
                case "limit" -> {
                    ensureArgCount(stepName, args, 1);
                    int parsedLimit;
                    try {
                        parsedLimit = Integer.parseInt(args.get(0).trim());
                    } catch (NumberFormatException nfe) {
                        throw new IllegalArgumentException("limit() must be numeric");
                    }
                    if (!hopsSeen) {
                        preHopLimit = parsedLimit;
                    } else {
                        limit = parsedLimit;
                    }
                }
                case "count" -> {
                    ensureArgCount(stepName, args, 0);
                    countRequested = true;
                }
                case "groupCount" -> {
                    ensureArgCount(stepName, args, 0);
                    if (groupCountSeen) {
                        throw new IllegalArgumentException("Only a single groupCount() step is supported");
                    }
                    groupCountSeen = true;
                }
                case "project" -> {
                    if (args.isEmpty()) {
                        throw new IllegalArgumentException("project expects at least 1 argument");
                    }
                    if (!projectAliases.isEmpty()) {
                        throw new IllegalArgumentException("Only a single project(...) step is supported");
                    }
                    for (String arg : args) {
                        String a = unquote(arg);
                        if (a.isBlank()) {
                            throw new IllegalArgumentException("project aliases must be non-empty");
                        }
                        projectAliases.add(a);
                    }
                }
                case "outE" -> {
                    if (args.size() > 1) {
                        throw new IllegalArgumentException("outE expects 0 or 1 argument(s)");
                    }
                    hops.add(new HopStep("outE", args.isEmpty() ? List.of() : List.of(unquote(args.get(0)))));
                }
                case "inE" -> {
                    if (args.size() > 1) {
                        throw new IllegalArgumentException("inE expects 0 or 1 argument(s)");
                    }
                    hops.add(new HopStep("inE", args.isEmpty() ? List.of() : List.of(unquote(args.get(0)))));
                }
                case "as" -> {
                    ensureArgCount(stepName, args, 1);
                    String aliasName = unquote(args.get(0));
                    if (aliasName.isBlank()) {
                        throw new IllegalArgumentException("as() label must be non-empty");
                    }
                    asAliases.add(new AsAlias(aliasName, hops.size()));
                }
                case "select" -> {
                    if (args.isEmpty()) {
                        throw new IllegalArgumentException("select expects at least 1 argument");
                    }
                    if (selectSeen) {
                        throw new IllegalArgumentException("Only a single select(...) step is supported");
                    }
                    selectSeen = true;
                    for (String arg : args) {
                        selectAliases.add(unquote(arg));
                    }
                }
                case "where" -> {
                    if (whereClause != null) {
                        throw new IllegalArgumentException("Only a single where() step is supported");
                    }
                    String rawWhere = parsedArgs.args().trim();
                    whereClause = parseWhere(rawWhere);
                }
                case "dedup" -> {
                    ensureArgCount(stepName, args, 0);
                    dedupRequested = true;
                }
                case "path" -> {
                    ensureArgCount(stepName, args, 0);
                    pathSeen = true;
                }
                case "by" -> {
                    if (args.isEmpty()) {
                        throw new IllegalArgumentException("by(...) expects at least 1 argument");
                    }
                    String byRaw = parsedArgs.args().trim();
                    if (selectSeen) {
                        if (selectByExpressions.size() >= selectAliases.size()) {
                            throw new IllegalArgumentException("select(...) has more by(...) modulators than selected aliases");
                        }
                        selectByExpressions.add(byRaw);
                    } else if (orderSeen && orderByProperty == null) {
                        if (byRaw.contains("select(") && byRaw.contains("Order.")) {
                            int selectStart = byRaw.indexOf("select(") + 7;
                            int selectEnd = byRaw.indexOf(")", selectStart);
                            orderByProperty = unquote(byRaw.substring(selectStart, selectEnd));
                            orderDirection = byRaw.contains("Order.desc") ? "DESC" : "ASC";
                        } else {
                            orderByProperty = unquote(byRaw);
                            orderDirection = "ASC";
                        }
                    } else if (groupCountSeen && groupCountProperty == null) {
                        ensureArgCount(stepName, args, 1);
                        groupCountProperty = unquote(args.get(0));
                        if (groupCountProperty.isBlank()) {
                            throw new IllegalArgumentException("by(...) property must be non-empty");
                        }
                    } else if (!projectAliases.isEmpty()) {
                        if (byExpressions.size() >= projectAliases.size()) {
                            throw new IllegalArgumentException("project(...) has more by(...) modulators than projected fields");
                        }
                        byExpressions.add(byRaw);
                    } else if (pathSeen) {
                        // path().by('prop'): multiple by() modulators — one per hop vertex
                        ensureArgCount(stepName, args, 1);
                        pathByProperties.add(unquote(args.get(0)));
                    } else {
                        throw new IllegalArgumentException("by(...) is only supported after project(...), groupCount(...), or order(...)");
                    }
                }
                case "order" -> {
                    ensureArgCount(stepName, args, 0);
                    if (orderSeen) {
                        throw new IllegalArgumentException("Only a single order() step is supported");
                    }
                    orderSeen = true;
                }
                default -> throw new IllegalArgumentException("Unsupported step: " + stepName);
            }

            index = parsedArgs.nextIndex();
        }

        if (pendingRepeatHop != null) {
            throw new IllegalArgumentException("repeat(...) must be followed by times(n)");
        }

        if (!projectAliases.isEmpty() && byExpressions.size() != projectAliases.size()) {
            throw new IllegalArgumentException("project(...) requires one by(...) modulator per projected field");
        }

        // Back-compat: if only one path().by() given, also populate valuesProperty for single-by path queries
        if (pathSeen && pathByProperties.size() == 1 && valuesProperty == null) {
            valuesProperty = pathByProperties.get(0);
        }

        List<ProjectionField> projections = new ArrayList<>();
        for (int i = 0; i < projectAliases.size(); i++) {
            projections.add(parseProjection(projectAliases.get(i), byExpressions.get(i)));
        }

        // Compile select fields: match each alias to its by() property modulator (if provided)
        List<SelectField> selectFields = new ArrayList<>();
        String sharedSelectBy = null;
        if (selectAliases.size() > 1 && selectByExpressions.size() == 1) {
            sharedSelectBy = unquote(selectByExpressions.get(0));
        }
        for (int i = 0; i < selectAliases.size(); i++) {
            String selectAlias = selectAliases.get(i);
            String property = sharedSelectBy != null
                    ? sharedSelectBy
                    : (i < selectByExpressions.size() ? unquote(selectByExpressions.get(i)) : null);
            selectFields.add(new SelectField(selectAlias, property));
        }

        return new ParsedTraversal(label, filters, valuesProperty, limit, preHopLimit, hops, countRequested, projections,
                groupCountProperty, orderByProperty, orderDirection, asAliases, selectFields, whereClause, dedupRequested,
                pathSeen, pathByProperties, simplePathRequested);
    }

    private static final Pattern EDGE_DEGREE_PATTERN =
            Pattern.compile("^(outE|inE)\\(['\"]([^'\"]+)['\"]\\)((?:\\.has\\(['\"][^'\"]+['\"],\\s*['\"][^'\"]*['\"]\\))*)\\s*\\.count\\(\\)$");

    /** Matches: out('label').has('p','v')[...].count()  or  in('label').has(...).count() */
    private static final Pattern VERTEX_COUNT_PATTERN =
            Pattern.compile("^(out|in)\\(['\"]([^'\"]+)['\"]\\)((?:\\.has\\(['\"][^'\"]+['\"],\\s*['\"][^'\"]*['\"]\\))*)\\s*\\.count\\(\\)$");

    private ProjectionField parseProjection(String alias, String byExpression) {
        String expression = byExpression == null ? "" : byExpression.trim();

        // Check for outE/inE count patterns with optional has() filters
        Matcher degreeMatcher = EDGE_DEGREE_PATTERN.matcher(expression);
        if (degreeMatcher.matches()) {
            String direction = degreeMatcher.group(1).equals("outE") ? "out" : "in";
            String edgeLabel = degreeMatcher.group(2);
            String hasChain = degreeMatcher.group(3);
            List<HasFilter> degreeFilters = parseHasChain(hasChain);
            String encodedProperty = direction + ":" + edgeLabel;
            if (!degreeFilters.isEmpty()) {
                StringBuilder encoded = new StringBuilder(encodedProperty);
                for (HasFilter f : degreeFilters) {
                    encoded.append(":").append(f.property()).append("=").append(f.value());
                }
                encodedProperty = encoded.toString();
            }
            return new ProjectionField(alias, ProjectionKind.EDGE_DEGREE, encodedProperty);
        }

        // out('label').has('prop','val').count() / in('label').has(...).count()
        Matcher vertexCountMatcher = VERTEX_COUNT_PATTERN.matcher(expression);
        if (vertexCountMatcher.matches()) {
            String direction  = vertexCountMatcher.group(1);
            String edgeLabel  = vertexCountMatcher.group(2);
            String hasChain   = vertexCountMatcher.group(3);
            List<HasFilter> vcFilters = parseHasChain(hasChain);
            StringBuilder encoded = new StringBuilder(direction).append(":").append(edgeLabel);
            for (HasFilter f : vcFilters) {
                encoded.append(":").append(f.property()).append("=").append(f.value());
            }
            ProjectionKind kind = "out".equals(direction) ? ProjectionKind.OUT_VERTEX_COUNT : ProjectionKind.IN_VERTEX_COUNT;
            return new ProjectionField(alias, kind, encoded.toString());
        }

        // out('label').values('prop').fold()  /  in('label').values('prop').fold()
        Matcher neighborValuesMatcher = NEIGHBOR_VALUES_FOLD_PATTERN.matcher(expression);
        if (neighborValuesMatcher.matches()) {
            String direction  = neighborValuesMatcher.group(1); // "out" or "in"
            String labelsRaw  = neighborValuesMatcher.group(2); // may be 'L1' or 'L1','L2'
            String prop       = neighborValuesMatcher.group(3);
            // Use the first label for the join (multi-label UNION not yet supported here)
            List<String> labelList = splitArgs(labelsRaw).stream().map(this::unquote).toList();
            String edgeLabel = labelList.get(0);
            // Encode as "direction:edgeLabel:property"
            String encoded = direction + ":" + edgeLabel + ":" + prop;
            ProjectionKind kind = "out".equals(direction)
                    ? ProjectionKind.OUT_NEIGHBOR_PROPERTY
                    : ProjectionKind.IN_NEIGHBOR_PROPERTY;
            return new ProjectionField(alias, kind, encoded);
        }

        // outV().values('prop')  /  inV().values('prop')
        Matcher endpointMatcher = ENDPOINT_VALUES_PATTERN.matcher(expression);
        if (endpointMatcher.matches()) {
            String endpoint = endpointMatcher.group(1);
            String property = unquote(endpointMatcher.group(2));
            if (property.isBlank()) {
                throw new IllegalArgumentException("by(outV().values(...)) and by(inV().values(...)) require a property name");
            }
            ProjectionKind kind = "outV".equals(endpoint)
                    ? ProjectionKind.OUT_VERTEX_PROPERTY
                    : ProjectionKind.IN_VERTEX_PROPERTY;
            return new ProjectionField(alias, kind, property);
        }

        if (expression.contains("(")) {
            throw new IllegalArgumentException("Unsupported by() projection expression: " + expression);
        }

        String property = unquote(expression);
        if (property.isBlank()) {
            throw new IllegalArgumentException("by(...) property must be non-empty");
        }
        return new ProjectionField(alias, ProjectionKind.EDGE_PROPERTY, property);
    }

    private VertexMapping resolveVertexMappingForEdgeProjection(MappingConfig mappingConfig) {
        if (mappingConfig.vertices().size() == 1) {
            return mappingConfig.vertices().values().iterator().next();
        }
        return null;
    }

    /**
     * Finds the vertex mapping that contains ALL of the requested property names.
     * Used as a fallback when the edge-column-name heuristic cannot resolve the vertex type.
     * Returns the first match, or null if no mapping contains all properties (or list is empty).
     */
    private VertexMapping resolveVertexMappingByProperties(MappingConfig mappingConfig, List<String> propertyNames) {
        if (propertyNames.isEmpty()) return null;
        for (VertexMapping vm : mappingConfig.vertices().values()) {
            if (propertyNames.stream().allMatch(p -> vm.properties().containsKey(p))) {
                return vm;
            }
        }
        return null;
    }

    /**
     * Resolves the vertex mapping for the destination of a single hop edge traversal.
     * Falls back to {@code defaultMapping} when no better match is found.
     */
    private VertexMapping resolveTargetVertexMapping(MappingConfig mappingConfig,
                                                     EdgeMapping edgeMapping,
                                                     boolean outDirection) {
        // Single vertex type — unambiguous
        if (mappingConfig.vertices().size() == 1) {
            return mappingConfig.vertices().values().iterator().next();
        }

        // The target column encodes the destination entity, e.g.:
        //   "alert_id"       → target is the vertex whose table contains "alert"
        //   "transaction_id" → target is "transaction"
        //   "bank_id"        → target is "bank"
        //   "country_id"     → target is "country"
        //
        // Strategy: strip trailing "_id" from the column name to get the entity stem,
        // then find the vertex mapping whose table name contains that stem.
        String targetCol = outDirection ? edgeMapping.inColumn() : edgeMapping.outColumn();

        // Derive stem: "alert_id" → "alert", "country_id" → "country"
        String stem = targetCol.replaceAll("_id$", "").replaceAll("^.*_", "");

        // 1. Best match: vertex table contains the stem (e.g. "aml_alerts" contains "alert")
        for (Map.Entry<String, VertexMapping> entry : mappingConfig.vertices().entrySet()) {
            String tableStem = entry.getValue().table().replaceAll("^[a-z]+_", ""); // strip "aml_"
            if (tableStem.equals(stem) || tableStem.startsWith(stem)) {
                return entry.getValue();
            }
        }

        // 2. Fallback: vertex label name matches the stem (case-insensitive)
        for (Map.Entry<String, VertexMapping> entry : mappingConfig.vertices().entrySet()) {
            if (entry.getKey().equalsIgnoreCase(stem)) {
                return entry.getValue();
            }
        }

        // 3. Last resort: vertex idColumn exactly matches the target column
        for (VertexMapping vm : mappingConfig.vertices().values()) {
            if (targetCol.equals(vm.idColumn())) {
                return vm;
            }
        }

        return null;
    }

    private String quoteAlias(String alias) {
        String escaped = alias.replace("\"", "\"\"");
        return "\"" + escaped + "\"";
    }

    private static final Pattern HAS_CHAIN_ITEM = Pattern.compile("\\.has\\('([^']+)','([^']+)'\\)");

    private List<HasFilter> parseHasChain(String hasChain) {
        List<HasFilter> filters = new ArrayList<>();
        if (hasChain == null || hasChain.isBlank()) {
            return filters;
        }
        Matcher m = HAS_CHAIN_ITEM.matcher(hasChain);
        while (m.find()) {
            filters.add(new HasFilter(m.group(1), m.group(2)));
        }
        return filters;
    }

    private record RepeatHopResult(HopStep hop, boolean simplePathDetected) {}

    private RepeatHopResult parseRepeatHop(String repeatBodyRaw) {
        // repeatBodyRaw is the full args string of repeat(...), e.g.:
        //   "out('LINK').simplePath()"
        //   "out('L1','L2').simplePath()"
        //   "both('T')"
        String text = repeatBodyRaw == null ? "" : repeatBodyRaw.trim();
        boolean hasSimplePath = text.endsWith(".simplePath()");
        if (hasSimplePath) {
            text = text.substring(0, text.length() - ".simplePath()".length()).trim();
        }
        if (!text.endsWith(")")) {
            throw new IllegalArgumentException("Only repeat(out(...)), repeat(in(...)), or repeat(both(...)) is supported");
        }

        String direction;
        int openParenIndex;
        if (text.startsWith("out(")) {
            direction = "out";
            openParenIndex = 3;
        } else if (text.startsWith("in(")) {
            direction = "in";
            openParenIndex = 2;
        } else if (text.startsWith("both(")) {
            direction = "both";
            openParenIndex = 4;
        } else {
            throw new IllegalArgumentException("Only repeat(out(...)), repeat(in(...)), or repeat(both(...)) is supported");
        }

        String inner = text.substring(openParenIndex + 1, text.length() - 1).trim();
        if (inner.isEmpty()) {
            return new RepeatHopResult(new HopStep(direction, List.of()), hasSimplePath);
        }
        // Parse all comma-separated labels (each may be quoted)
        List<String> labels = splitArgs(inner).stream().map(this::unquote).toList();
        return new RepeatHopResult(new HopStep(direction, labels), hasSimplePath);
    }

    // Patterns for where() step parsing
    // where('a', neq('b'))
    private static final Pattern WHERE_NEQ_PATTERN =
            Pattern.compile("^'([^']+)'\\s*,\\s*neq\\('([^']+)'\\)$");
    // where(select('alias').is(gt(n))) or where(select('alias').is(gte(n)))
    private static final Pattern WHERE_SELECT_IS_GT_PATTERN =
            Pattern.compile("^select\\(['\"]([^'\"]+)['\"]\\)\\.is\\(gte?\\(([^)]+)\\)\\)$");
    // where(outE('TRANSFER').has('isLaundering','1'))  — also matches double-quoted labels
    private static final Pattern WHERE_EDGE_EXISTS_PATTERN =
            Pattern.compile("^(outE|inE|bothE)\\(['\"]([^'\"]+)['\"]\\)((?:\\.has\\(['\"][^'\"]+['\"],\\s*['\"][^'\"]*['\"]\\))*)$");
    // where(out('SENT_VIA').has('fatfBlacklist','true'))  — vertex reachable via edge with property filter
    private static final Pattern WHERE_OUT_NEIGHBOR_HAS_PATTERN =
            Pattern.compile("^(out|in)\\(['\"]([^'\"]+)['\"]\\)((?:\\.has\\(['\"][^'\"]+['\"],\\s*['\"][^'\"]*['\"]\\))+)$");

    private WhereClause parseWhere(String rawWhere) {
        // Form 1: where('a', neq('b'))
        Matcher neqMatcher = WHERE_NEQ_PATTERN.matcher(rawWhere);
        if (neqMatcher.matches()) {
            return new WhereClause(WhereKind.NEQ_ALIAS, neqMatcher.group(1), neqMatcher.group(2), List.of());
        }
        // Form 2: where(select('alias').is(gt(n))) or where(select('alias').is(gte(n)))
        Matcher gtMatcher = WHERE_SELECT_IS_GT_PATTERN.matcher(rawWhere);
        if (gtMatcher.matches()) {
            String alias = gtMatcher.group(1);
            String value = gtMatcher.group(2).trim();
            // encode gte vs gt in the right field
            boolean isGte = rawWhere.contains(".is(gte(");
            return new WhereClause(isGte ? WhereKind.PROJECT_GTE : WhereKind.PROJECT_GT, alias, value, List.of());
        }
        // Form 3: where(outE/inE/bothE('label').has(...))
        Matcher edgeExistsMatcher = WHERE_EDGE_EXISTS_PATTERN.matcher(rawWhere);
        if (edgeExistsMatcher.matches()) {
            String direction = edgeExistsMatcher.group(1);
            String edgeLabel = edgeExistsMatcher.group(2);
            String hasChain = edgeExistsMatcher.group(3);
            return new WhereClause(WhereKind.EDGE_EXISTS, direction, edgeLabel, parseHasChain(hasChain));
        }
        // Form 4: where(out('label').has('prop','val')) — neighbor vertex property existence
        Matcher neighborMatcher = WHERE_OUT_NEIGHBOR_HAS_PATTERN.matcher(rawWhere);
        if (neighborMatcher.matches()) {
            String direction = neighborMatcher.group(1); // "out" or "in"
            String edgeLabel = neighborMatcher.group(2);
            String hasChain  = neighborMatcher.group(3);
            WhereKind kind = "out".equals(direction) ? WhereKind.OUT_NEIGHBOR_HAS : WhereKind.IN_NEIGHBOR_HAS;
            return new WhereClause(kind, direction, edgeLabel, parseHasChain(hasChain));
        }
        throw new IllegalArgumentException("Unsupported where() predicate: " + rawWhere);
    }

    private static void ensureArgCount(String step, List<String> args, int expected) {
        if (args.size() != expected) {
            throw new IllegalArgumentException(step + " expects " + expected + " argument(s)");
        }
    }

    private ParsedArgs readArgs(String query, int openingParenIndex) {
        StringBuilder argsBuilder = new StringBuilder();
        boolean inSingleQuote = false;
        boolean inDoubleQuote = false;
        int nestingDepth = 1;
        int index = openingParenIndex + 1;

        while (index < query.length()) {
            char c = query.charAt(index);
            if (c == '\'' && !inDoubleQuote) {
                inSingleQuote = !inSingleQuote;
                argsBuilder.append(c);
            } else if (c == '"' && !inSingleQuote) {
                inDoubleQuote = !inDoubleQuote;
                argsBuilder.append(c);
            } else if (!inSingleQuote && !inDoubleQuote && c == '(') {
                nestingDepth++;
                argsBuilder.append(c);
            } else if (!inSingleQuote && !inDoubleQuote && c == ')') {
                nestingDepth--;
                if (nestingDepth == 0) {
                    return new ParsedArgs(argsBuilder.toString(), index + 1);
                }
                argsBuilder.append(c);
            } else {
                argsBuilder.append(c);
            }
            index++;
        }
        throw new IllegalArgumentException("Unclosed step arguments in query");
    }

    private List<String> splitArgs(String argsText) {
        List<String> args = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inSingleQuote = false;
        boolean inDoubleQuote = false;

        for (int i = 0; i < argsText.length(); i++) {
            char c = argsText.charAt(i);
            if (c == '\'' && !inDoubleQuote) {
                inSingleQuote = !inSingleQuote;
                current.append(c);
            } else if (c == '"' && !inSingleQuote) {
                inDoubleQuote = !inDoubleQuote;
                current.append(c);
            } else if (c == ',' && !inSingleQuote && !inDoubleQuote) {
                args.add(current.toString().trim());
                current.setLength(0);
            } else {
                current.append(c);
            }
        }

        if (!argsText.isBlank()) {
            args.add(current.toString().trim());
        }
        return args;
    }

    private String unquote(String text) {
        String trimmed = text.trim();
        if ((trimmed.startsWith("\"") && trimmed.endsWith("\"")) ||
                (trimmed.startsWith("'") && trimmed.endsWith("'"))) {
            return trimmed.substring(1, trimmed.length() - 1);
        }
        return trimmed;
    }

    private record ParsedArgs(String args, int nextIndex) {}

    private record RootStep(boolean vertexQuery, int nextIndex, String rootIdFilter) {}

    private record HopStep(String direction, List<String> labels) {
        /** Convenience constructor for a single-label hop (used extensively in SQL building). */
        HopStep(String direction, String singleLabel) {
            this(direction, singleLabel == null ? List.of() : List.of(singleLabel));
        }
        /** Returns the single label, or null if no labels are set. Throws if multiple labels. */
        String singleLabel() {
            if (labels.isEmpty()) return null;
            if (labels.size() > 1) throw new IllegalStateException("singleLabel() called on multi-label HopStep: " + labels);
            return labels.get(0);
        }
    }

    private record ParsedTraversal(String label,
                                   List<HasFilter> filters,
                                   String valueProperty,
                                   Integer limit,
                                   Integer preHopLimit,
                                   List<HopStep> hops,
                                   boolean countRequested,
                                   List<ProjectionField> projections,
                                   String groupCountProperty,
                                   String orderByProperty,
                                   String orderDirection,
                                   List<AsAlias> asAliases,
                                   List<SelectField> selectFields,
                                   WhereClause whereClause,
                                   boolean dedupRequested,
                                   boolean pathSeen,
                                   List<String> pathByProperties,
                                   boolean simplePathRequested) {
    }

    private enum ProjectionKind {
        EDGE_PROPERTY,
        OUT_VERTEX_PROPERTY,
        IN_VERTEX_PROPERTY,
        EDGE_DEGREE,
        OUT_VERTEX_COUNT,
        IN_VERTEX_COUNT,
        /** out('edgeLabel').values('prop').fold() — property from destination vertex via edge join */
        OUT_NEIGHBOR_PROPERTY,
        /** in('edgeLabel').values('prop').fold() — property from source vertex via edge join */
        IN_NEIGHBOR_PROPERTY
    }

    private record ProjectionField(String alias, ProjectionKind kind, String property) {
    }

    /** Marks the traversal position when `.as('label')` is encountered. */
    private record AsAlias(String label, int hopIndexAfter) {
    }

    /** One field emitted by a terminal .select('a','b').by('prop') chain. */
    private record SelectField(String alias, String property) {
    }

    /**
     * A WHERE predicate.
     * <ul>
     *   <li>kind=NEQ_ALIAS: {@code where('a', neq('b'))} – two traversal aliases must differ.</li>
     *   <li>kind=PROJECT_GT: {@code where(select('alias').is(gt(n)))} – projected sub-value > n.</li>
     * </ul>
     */
    private record WhereClause(WhereKind kind, String left, String right, List<HasFilter> filters) {
    }

    private enum WhereKind {
        NEQ_ALIAS,
        PROJECT_GT,
        PROJECT_GTE,
        EDGE_EXISTS,
        /** where(out('label').has('prop','val')) — anchor vertex has an out-neighbor with the given property */
        OUT_NEIGHBOR_HAS,
        /** where(in('label').has('prop','val')) — anchor vertex has an in-neighbor with the given property */
        IN_NEIGHBOR_HAS
    }
}

