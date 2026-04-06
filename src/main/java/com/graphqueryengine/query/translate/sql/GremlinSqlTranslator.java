package com.graphqueryengine.query.translate.sql;

import com.graphqueryengine.mapping.EdgeMapping;
import com.graphqueryengine.mapping.MappingConfig;
import com.graphqueryengine.mapping.VertexMapping;
import com.graphqueryengine.query.api.QueryPlan;
import com.graphqueryengine.query.api.TranslationResult;
import com.graphqueryengine.query.parser.LegacyGremlinTraversalParser;
import com.graphqueryengine.query.parser.model.GremlinParseResult;
import com.graphqueryengine.query.translate.sql.dialect.SqlDialect;
import com.graphqueryengine.query.translate.sql.dialect.StandardSqlDialect;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GremlinSqlTranslator {
   private static final Pattern ENDPOINT_VALUES_PATTERN = Pattern.compile("^(outV|inV)\\(\\)\\.values\\((.+)\\)$");
   private static final Pattern NEIGHBOR_VALUES_FOLD_PATTERN = Pattern.compile("^(out|in)\\((.+)\\)\\.values\\(['\"]([^'\"]+)['\"]\\)\\.fold\\(\\)$");
   private static final Pattern EDGE_DEGREE_PATTERN = Pattern.compile("^(outE|inE)\\(['\"]([^'\"]+)['\"]\\)((?:\\.has\\(['\"][^'\"]+['\"],\\s*['\"][^'\"]*['\"]\\))*)\\s*\\.count\\(\\)$");
   private static final Pattern VERTEX_COUNT_PATTERN = Pattern.compile("^(out|in)\\(['\"]([^'\"]+)['\"]\\)((?:\\.has\\(['\"][^'\"]+['\"],\\s*['\"][^'\"]*['\"]\\))*)\\s*\\.count\\(\\)$");
   private static final Pattern HAS_CHAIN_ITEM = Pattern.compile("\\.has\\('([^']+)','([^']+)'\\)");
   private static final Pattern WHERE_NEQ_PATTERN = Pattern.compile("^'([^']+)'\\s*,\\s*neq\\('([^']+)'\\)$");
   private static final Pattern WHERE_EQ_ALIAS_PATTERN = Pattern.compile("^eq\\(['\"]([^'\"]+)['\"]\\)$");
   private static final Pattern WHERE_SELECT_IS_GT_PATTERN = Pattern.compile("^select\\(['\"]([^'\"]+)['\"]\\)\\.is\\(gte?\\(([^)]+)\\)\\)$");
   private static final Pattern WHERE_EDGE_EXISTS_PATTERN = Pattern.compile("^(outE|inE|bothE)\\(['\"]([^'\"]+)['\"]\\)((?:\\.has\\(['\"][^'\"]+['\"],\\s*['\"][^'\"]*['\"]\\))*)$");
   private static final Pattern WHERE_EDGE_COUNT_IS_PATTERN = Pattern.compile("^(outE|inE|bothE)\\(['\"]([^'\"]+)['\"]\\)((?:\\.has\\(['\"][^'\"]+['\"],\\s*['\"][^'\"]*['\"]\\))*)\\s*\\.count\\(\\)\\.is\\((?:(eq|neq|gt|gte|lt|lte)\\(([^)]+)\\)|(-?\\d+))\\)$");
   private static final Pattern WHERE_OUT_NEIGHBOR_HAS_PATTERN = Pattern.compile("^(out|in)\\(['\"]([^'\"]+)['\"]\\)((?:\\.has\\(['\"][^'\"]+['\"],\\s*['\"][^'\"]*['\"]\\))*)$");
   private static final Pattern HAS_PREDICATE_PATTERN = Pattern.compile("^(gt|gte|lt|lte|neq|eq)\\((.+)\\)$");
   private static final Pattern CHOOSE_VALUES_IS_CONSTANT_PATTERN = Pattern.compile("^choose\\(\\s*values\\(['\"]([^'\"]+)['\"]\\)\\.is\\((gt|gte|lt|lte|eq|neq)\\(([^)]+)\\)\\)\\s*,\\s*constant\\(([^)]+)\\)\\s*,\\s*constant\\(([^)]+)\\)\\s*\\)$");
   private static final Pattern GROUP_COUNT_SELECT_BY_PATTERN = Pattern.compile("^select\\(['\"]([^'\"]+)['\"]\\)\\.by\\(['\"]([^'\"]+)['\"]\\)$");

   private final SqlDialect dialect;

   public GremlinSqlTranslator() {
      this(new StandardSqlDialect());
   }

   public GremlinSqlTranslator(SqlDialect dialect) {
      this.dialect = dialect;
   }

    public TranslationResult translate(String gremlin, MappingConfig mappingConfig) {
       GremlinParseResult parsed = (new LegacyGremlinTraversalParser()).parse(gremlin);
       return this.translate(parsed, mappingConfig);
    }

    public TranslationResult translate(GremlinParseResult parsed, MappingConfig mappingConfig) {
       if (parsed == null) {
          throw new IllegalArgumentException("Parsed Gremlin query is required");
       }
       ParsedTraversal parsedTraversal = this.parseSteps(parsed.steps(), parsed.rootIdFilter());
       return parsed.vertexQuery()
               ? this.buildVertexSql(parsedTraversal, mappingConfig)
               : this.buildEdgeSql(parsedTraversal, mappingConfig);
    }

    /**
     * Translate a Gremlin query string to SQL, also computing and attaching a {@link QueryPlan}
     * showing all planner-stage decisions before SQL rendering.
     */
    public TranslationResult translateWithPlan(String gremlin, MappingConfig mappingConfig) {
       GremlinParseResult parsed = (new LegacyGremlinTraversalParser()).parse(gremlin);
       return this.translateWithPlan(parsed, mappingConfig);
    }

    /**
     * Like {@link #translate} but also attaches a {@link QueryPlan} to the result
     * so callers can inspect every resolved mapping decision before SQL rendering.
     */
    public TranslationResult translateWithPlan(GremlinParseResult parsed, MappingConfig mappingConfig) {
       if (parsed == null) {
          throw new IllegalArgumentException("Parsed Gremlin query is required");
       }
       ParsedTraversal parsedTraversal = this.parseSteps(parsed.steps(), parsed.rootIdFilter());
       TranslationResult base = parsed.vertexQuery()
               ? this.buildVertexSql(parsedTraversal, mappingConfig)
               : this.buildEdgeSql(parsedTraversal, mappingConfig);
       QueryPlan plan = this.buildPlan(parsed.vertexQuery(), parsedTraversal, mappingConfig);
       return new TranslationResult(base.sql(), base.parameters(), plan);
    }

   /** Builds a {@link QueryPlan} without producing SQL. */
   public QueryPlan plan(String gremlin, MappingConfig mappingConfig) {
      GremlinParseResult parsed = (new LegacyGremlinTraversalParser()).parse(gremlin);
      return planFromParsed(parsed, mappingConfig);
   }

   /** Builds a {@link QueryPlan} from an already-parsed result without producing SQL. */
   public QueryPlan planFromParsed(GremlinParseResult parsed, MappingConfig mappingConfig) {
      if (parsed == null) {
         throw new IllegalArgumentException("Parsed Gremlin query is required");
      }
      ParsedTraversal parsedTraversal = this.parseSteps(parsed.steps(), parsed.rootIdFilter());
      return buildPlan(parsed.vertexQuery(), parsedTraversal, mappingConfig);
   }

   // ── Internal plan builder ─────────────────────────────────────────────────

   private QueryPlan buildPlan(boolean vertexQuery, ParsedTraversal t, MappingConfig mappingConfig) {
      String rootType = vertexQuery ? "vertex" : "edge";

      // Resolve root label and table
      String rootLabel = t.label();
      String rootTable;
      if (vertexQuery) {
         if (rootLabel == null && !mappingConfig.vertices().isEmpty()) {
            try { rootLabel = this.resolveSingleLabel(mappingConfig.vertices(), "vertex"); } catch (Exception ignored) {}
         }
         VertexMapping vm = rootLabel != null ? mappingConfig.vertices().get(rootLabel) : null;
         rootTable = vm != null ? vm.table() : null;
      } else {
         if (rootLabel == null && !mappingConfig.edges().isEmpty()) {
            try { rootLabel = this.resolveSingleLabel(mappingConfig.edges(), "edge"); } catch (Exception ignored) {}
         }
         EdgeMapping em = rootLabel != null ? mappingConfig.edges().get(rootLabel) : null;
         rootTable = em != null ? em.table() : null;
      }

      // Filters
      List<QueryPlan.FilterPlan> filterPlans = t.filters().stream()
              .map(f -> new QueryPlan.FilterPlan(f.property(), f.operator(), f.value()))
              .toList();

      // Hops
      List<QueryPlan.HopPlan> hopPlans = new ArrayList<>();
      for (HopStep hop : t.hops()) {
         String edgeTable = null;
         String targetTable = null;
         String targetLabel = null;
         if (!hop.labels().isEmpty()) {
            String firstLabel = hop.labels().get(0);
            EdgeMapping em = mappingConfig.edges().get(firstLabel);
            if (em != null) {
               edgeTable = em.table();
               boolean outDir = "out".equals(hop.direction()) || "outE".equals(hop.direction());
               VertexMapping tvm = this.resolveTargetVertexMapping(mappingConfig, em, outDir);
               if (tvm != null) {
                  targetTable = tvm.table();
                  // find its label key
                  for (Map.Entry<String, VertexMapping> e : mappingConfig.vertices().entrySet()) {
                     if (e.getValue() == tvm) { targetLabel = e.getKey(); break; }
                  }
               }
            }
         }
         hopPlans.add(new QueryPlan.HopPlan(hop.direction(), hop.labels(), edgeTable, targetTable, targetLabel));
      }

      // Aggregation
      String aggregation = null;
      String aggregationProperty = null;
      if (t.countRequested()) aggregation = "count";
      else if (t.sumRequested()) { aggregation = "sum"; aggregationProperty = t.valueProperty(); }
      else if (t.meanRequested()) { aggregation = "mean"; aggregationProperty = t.valueProperty(); }
      else if (t.groupCountProperty() != null) { aggregation = "groupCount"; aggregationProperty = t.groupCountProperty(); }
      else if (t.groupCountKeySpec() != null) { aggregation = "groupCount"; }

      // Projections
      List<QueryPlan.ProjectionPlan> projPlans = t.projections().stream()
              .map(p -> new QueryPlan.ProjectionPlan(p.alias(), p.kind().name(), p.property()))
              .toList();

      // Where
      QueryPlan.WherePlan wherePlan = null;
      if (t.whereClause() != null) {
         WhereClause wc = t.whereClause();
         List<QueryPlan.FilterPlan> wFilters = wc.filters().stream()
                 .map(f -> new QueryPlan.FilterPlan(f.property(), f.operator(), f.value()))
                 .toList();
         wherePlan = new QueryPlan.WherePlan(wc.kind().name(), wc.left(), wc.right(),
                 wFilters.isEmpty() ? null : wFilters);
      }

      // As aliases
      List<QueryPlan.AliasPlan> aliasPlan = t.asAliases().stream()
              .map(a -> new QueryPlan.AliasPlan(a.label(), a.hopIndexAfter()))
              .toList();

      // Select fields
      List<QueryPlan.SelectPlan> selectPlan = t.selectFields().stream()
              .map(s -> new QueryPlan.SelectPlan(s.alias(), s.property()))
              .toList();

      return new QueryPlan(
              rootType,
              rootLabel,
              rootTable,
              dialect.getClass().getSimpleName(),
              filterPlans.isEmpty() ? null : filterPlans,
               (t.filters().isEmpty() || !"id".equals(t.filters().get(0).property())) ? null : t.filters().get(0).value(),
              hopPlans.isEmpty() ? null : hopPlans,
              t.simplePathRequested() ? true : null,
              aggregation,
              aggregationProperty,
              (t.valueProperty() != null && aggregation == null) ? t.valueProperty() : null,
              projPlans.isEmpty() ? null : projPlans,
              t.orderByProperty(),
              t.orderDirection(),
              t.orderByCountDesc() ? true : null,
              t.limit(),
              t.preHopLimit(),
              wherePlan,
              t.dedupRequested() ? true : null,
              (t.pathByProperties() == null || t.pathByProperties().isEmpty()) ? null : t.pathByProperties(),
              aliasPlan.isEmpty() ? null : aliasPlan,
              selectPlan.isEmpty() ? null : selectPlan
      );
   }

   // ── Missing methods and inner types (restored from committed version) ─────

   private TranslationResult buildVertexSql(final ParsedTraversal parsed, MappingConfig mappingConfig) {
      String label = parsed.label();
      if (label == null) {
         label = this.resolveSingleLabel(mappingConfig.vertices(), "vertex");
      }

      VertexMapping vertexMapping = mappingConfig.vertices().get(label);
      if (vertexMapping == null) {
         throw new IllegalArgumentException("No vertex mapping found for label: " + label);
      }
      Integer effectiveLimit = parsed.limit() != null ? parsed.limit() : parsed.preHopLimit();
      if (parsed.groupCountProperty() != null) {
         return this.buildVertexGroupCountSql(parsed, vertexMapping);
      } else if (parsed.groupCountKeySpec() != null) {
         return this.buildAliasKeyedGroupCountSql(parsed, mappingConfig, vertexMapping);
      } else if (!parsed.hops().isEmpty()) {
         return this.buildHopTraversalSql(parsed, mappingConfig, vertexMapping);
      } else if (!parsed.projections().isEmpty()) {
         return this.buildVertexProjectionSql(parsed, mappingConfig, vertexMapping);
      } else if (parsed.valueMapRequested()) {
         StringJoiner selectCols = new StringJoiner(", ");
         for (Map.Entry<String, String> entry : vertexMapping.properties().entrySet()) {
            selectCols.add(entry.getValue() + " AS " + this.quoteAlias(entry.getKey()));
         }

         List<Object> params = new ArrayList<>();
         StringBuilder sql = (new StringBuilder("SELECT "))
               .append(parsed.dedupRequested() ? "DISTINCT " : "")
               .append(selectCols)
               .append(" FROM ")
               .append(vertexMapping.table());
         this.appendWhereClauseForVertex(sql, params, parsed.filters(), vertexMapping);
         if (parsed.whereClause() != null) {
            if (this.isStructuredWherePredicate(parsed.whereClause())) {
               this.appendStructuredWherePredicate(sql, params, parsed.whereClause(), mappingConfig, vertexMapping, null, !parsed.filters().isEmpty());
            }
         }

         appendOrderBy(sql, parsed.orderByProperty(), parsed.orderDirection());
         appendLimit(sql, effectiveLimit);
         return new TranslationResult(sql.toString(), params);
      } else if (parsed.countRequested() && parsed.sumRequested()) {
         throw new IllegalArgumentException("count() cannot be combined with sum()");
      } else {
         String selectClause = parsed.countRequested() ? (parsed.dedupRequested() ? "COUNT(DISTINCT " + vertexMapping.idColumn() + ") AS count" : "COUNT(*) AS count") : (parsed.dedupRequested() ? "DISTINCT *" : "*");
         List<Object> params = new ArrayList<>();
         if (parsed.sumRequested()) {
            if (parsed.valueProperty() == null) {
               throw new IllegalArgumentException("sum() requires values('property') before aggregation");
            }

            String mappedColumn = this.mapVertexProperty(vertexMapping, parsed.valueProperty());
            selectClause = "SUM(" + mappedColumn + ") AS sum";
         }

         if (parsed.meanRequested()) {
            if (parsed.valueProperty() == null) {
               throw new IllegalArgumentException("mean() requires values('property') before aggregation");
            }

            String mappedColumn = this.mapVertexProperty(vertexMapping, parsed.valueProperty());
            selectClause = "AVG(" + mappedColumn + ") AS mean";
         }

         if (parsed.valueProperty() != null && !parsed.countRequested() && !parsed.sumRequested() && !parsed.meanRequested()) {
            String mappedColumn = this.mapVertexProperty(vertexMapping, parsed.valueProperty());
            selectClause = (parsed.dedupRequested() ? "DISTINCT " : "") + mappedColumn + " AS " + parsed.valueProperty();
         }

         if (parsed.sumRequested()) {
            String mappedColumn = this.mapVertexProperty(vertexMapping, parsed.valueProperty());
            selectClause = "SUM(" + mappedColumn + ") AS sum";
         }

         if (parsed.meanRequested()) {
            String mappedColumn = this.mapVertexProperty(vertexMapping, parsed.valueProperty());
            selectClause = "AVG(" + mappedColumn + ") AS mean";
         }

         StringBuilder sql = (new StringBuilder("SELECT ")).append(selectClause).append(" FROM ").append(vertexMapping.table());
         this.appendWhereClauseForVertex(sql, params, parsed.filters(), vertexMapping);
         if (parsed.whereClause() != null) {
            if (!this.isStructuredWherePredicate(parsed.whereClause())) {
               throw new IllegalArgumentException("Unsupported where() predicate for g.V(): " + parsed.whereClause().kind());
            }
            this.appendStructuredWherePredicate(sql, params, parsed.whereClause(), mappingConfig, vertexMapping, null, !parsed.filters().isEmpty());
         }

         appendOrderBy(sql, parsed.orderByProperty(), parsed.orderDirection());
         appendLimit(sql, effectiveLimit);
         return new TranslationResult(sql.toString(), params);
      }
   }

   private TranslationResult buildHopTraversalSql(ParsedTraversal parsed, MappingConfig mappingConfig, VertexMapping startVertexMapping) {
      List<Object> params = new ArrayList<>();
      boolean hasBothHop = parsed.hops().stream().anyMatch((h) -> "both".equals(h.direction()));
      if (hasBothHop) {
         return this.buildBothHopUnionSql(parsed, mappingConfig, startVertexMapping);
      } else {
         boolean hasMultiLabelHop = parsed.hops().stream().anyMatch((h) -> h.labels().size() > 1);
         if (hasMultiLabelHop) {
            return this.buildMultiLabelHopUnionSql(parsed, mappingConfig, startVertexMapping);
         } else {
            StringBuilder sql = new StringBuilder();
            int hopCount = parsed.hops().size();
            HopStep terminalHop = parsed.hops().get(hopCount - 1);
            boolean terminalEdgeHop = "outE".equals(terminalHop.direction()) || "inE".equals(terminalHop.direction()) || "bothE".equals(terminalHop.direction());
            String finalVertexAlias = "v" + hopCount;
            String finalEdgeAlias = "e" + hopCount;
            String idCol = startVertexMapping.idColumn();
            EdgeMapping finalEdgeMapping = terminalEdgeHop ? this.resolveEdgeMapping(terminalHop.singleLabel(), mappingConfig) : null;
            if (parsed.countRequested()) {
               if (parsed.dedupRequested()) {
                  String distinctAlias = terminalEdgeHop ? finalEdgeAlias : finalVertexAlias;
                  String distinctColumn = terminalEdgeHop ? finalEdgeMapping.idColumn() : idCol;
                  sql.append("SELECT COUNT(DISTINCT ").append(distinctAlias).append('.').append(distinctColumn).append(") AS count");
               } else {
                  sql.append("SELECT COUNT(*) AS count");
               }
            } else if (parsed.sumRequested()) {
               sql.append("SELECT ").append(this.buildHopSumExpression(parsed, mappingConfig, startVertexMapping, finalVertexAlias)).append(" AS sum");
            } else if (parsed.meanRequested()) {
               sql.append("SELECT ").append(this.buildHopMeanExpression(parsed, mappingConfig, startVertexMapping, finalVertexAlias)).append(" AS mean");
            } else if (!parsed.projections().isEmpty()) {
               VertexMapping finalVertexMapping = terminalEdgeHop
                     ? null
                     : this.resolveFinalHopVertexMapping(parsed.hops(), mappingConfig, startVertexMapping);
               StringJoiner projectionSelect = new StringJoiner(", ");

               for(ProjectionField projection : parsed.projections()) {
                  if (projection.kind() == GremlinSqlTranslator.ProjectionKind.IDENTITY) {
                     String alias = terminalEdgeHop ? finalEdgeAlias : finalVertexAlias;
                     String idColumn = terminalEdgeHop ? finalEdgeMapping.idColumn() : idCol;
                     projectionSelect.add(alias + "." + idColumn + " AS " + this.quoteAlias(projection.alias()));
                     continue;
                  }
                  if (projection.kind() != GremlinSqlTranslator.ProjectionKind.EDGE_PROPERTY) {
                     throw new IllegalArgumentException("Hop projection currently supports by('property') or by(identity()) only");
                  }

                  String mappedColumn = terminalEdgeHop
                        ? this.mapEdgeProperty(finalEdgeMapping, projection.property())
                        : this.mapVertexProperty(finalVertexMapping, projection.property());
                  String alias = terminalEdgeHop ? finalEdgeAlias : finalVertexAlias;
                  projectionSelect.add(alias + "." + mappedColumn + " AS " + this.quoteAlias(projection.alias()));
               }

               sql.append(parsed.dedupRequested() ? "SELECT DISTINCT " : "SELECT ").append(projectionSelect);
            } else if (parsed.pathSeen() && !parsed.pathByProperties().isEmpty()) {
               sql.append(this.buildPathSelectClause(parsed.pathByProperties(), hopCount, mappingConfig, startVertexMapping, parsed.hops()));
            } else if (parsed.valueMapRequested()) {
               if (terminalEdgeHop) {
                  sql.append(parsed.dedupRequested() ? "SELECT DISTINCT " : "SELECT ").append(this.buildAliasValueMapSelectForEdge(finalEdgeMapping, finalEdgeAlias));
               } else {
                  VertexMapping finalVm = this.resolveFinalHopVertexMapping(parsed.hops(), mappingConfig, startVertexMapping);
                  sql.append(parsed.dedupRequested() ? "SELECT DISTINCT " : "SELECT ").append(this.buildAliasValueMapSelectForVertex(finalVm, finalVertexAlias));
               }
            } else if (parsed.valueProperty() != null) {
               if (terminalEdgeHop) {
                  String mappedColumn = this.mapEdgeProperty(finalEdgeMapping, parsed.valueProperty());
                  sql.append("SELECT ").append(parsed.dedupRequested() ? "DISTINCT " : "").append(finalEdgeAlias).append('.').append(mappedColumn).append(" AS ").append(parsed.valueProperty());
               } else {
                  VertexMapping finalVertexMapping = this.resolveFinalHopVertexMapping(parsed.hops(), mappingConfig, startVertexMapping);
                  String mappedColumn = this.mapVertexProperty(finalVertexMapping, parsed.valueProperty());
                  sql.append("SELECT ").append(parsed.dedupRequested() ? "DISTINCT " : "").append(finalVertexAlias).append('.').append(mappedColumn).append(" AS ").append(parsed.valueProperty());
               }
            } else {
               sql.append(parsed.dedupRequested() ? "SELECT DISTINCT " : "SELECT ").append(terminalEdgeHop ? finalEdgeAlias : finalVertexAlias).append(".*");
            }

            boolean useStartSubquery = parsed.preHopLimit() != null;
            if (useStartSubquery) {
               StringBuilder startSubquery = (new StringBuilder("(SELECT * FROM ")).append(startVertexMapping.table());
               List<Object> subParams = new ArrayList<>();
               this.appendWhereClauseForVertex(startSubquery, subParams, parsed.filters(), startVertexMapping);
               if (parsed.whereClause() != null) {
                  if (!this.isStructuredWherePredicate(parsed.whereClause())) {
                     throw new IllegalArgumentException("Unsupported where() predicate for hop traversal: " + parsed.whereClause().kind());
                  }
                  this.appendStructuredWherePredicate(startSubquery, subParams, parsed.whereClause(), mappingConfig, startVertexMapping, null, !parsed.filters().isEmpty());
               }

               startSubquery.append(" LIMIT ").append(parsed.preHopLimit()).append(")");
               params.addAll(subParams);
               sql.append(" FROM ").append(startSubquery).append(" v0");
            } else {
               sql.append(" FROM ").append(startVertexMapping.table()).append(" v0");
            }

            for(int i = 0; i < hopCount; ++i) {
               HopStep hop = parsed.hops().get(i);
               EdgeMapping edgeMapping = this.resolveEdgeMapping(hop.singleLabel(), mappingConfig);
               String edgeAlias = "e" + (i + 1);
               String fromVertexAlias = "v" + i;
               String toVertexAlias = "v" + (i + 1);
               String fromId = fromVertexAlias + "." + idCol;
               String toId = toVertexAlias + "." + idCol;
               String edgeOut = edgeAlias + "." + edgeMapping.outColumn();
               String edgeIn = edgeAlias + "." + edgeMapping.inColumn();
               sql.append(" JOIN ").append(edgeMapping.table()).append(' ').append(edgeAlias);
               if ("bothE".equals(hop.direction())) {
                  sql.append(" ON (").append(edgeOut).append(" = ").append(fromId).append(" OR ").append(edgeIn).append(" = ").append(fromId).append(")");
               } else if ("out".equals(hop.direction()) || "outE".equals(hop.direction())) {
                  sql.append(" ON ").append(edgeOut).append(" = ").append(fromId);
               } else {
                  sql.append(" ON ").append(edgeIn).append(" = ").append(fromId);
               }

               if (!"outE".equals(hop.direction()) && !"inE".equals(hop.direction()) && !"bothE".equals(hop.direction())) {
                  VertexMapping toVertexMapping = this.resolveHopTargetVertexMapping(mappingConfig, edgeMapping, "out".equals(hop.direction()), startVertexMapping);
                  sql.append(" JOIN ").append(toVertexMapping.table()).append(' ').append(toVertexAlias);
                  if ("out".equals(hop.direction())) {
                     sql.append(" ON ").append(toId).append(" = ").append(edgeIn);
                  } else {
                     sql.append(" ON ").append(toId).append(" = ").append(edgeOut);
                  }
               }
            }

            boolean hasWhere = false;
            if (!useStartSubquery) {
               if (!parsed.filters().isEmpty()) {
                  hasWhere = this.appendHopFiltersWithTargetFallback(sql, params, parsed.filters(), startVertexMapping, finalVertexAlias, this.resolveFinalHopVertexMapping(parsed.hops(), mappingConfig, startVertexMapping), parsed, mappingConfig);
               }

               if (parsed.whereClause() != null) {
                  if (this.isStructuredWherePredicate(parsed.whereClause())) {
                     this.appendStructuredWherePredicate(sql, params, parsed.whereClause(), mappingConfig, startVertexMapping, "v0", hasWhere);
                  } else {
                     if (parsed.whereClause().kind() == GremlinSqlTranslator.WhereKind.EQ_ALIAS) {
                        this.appendEqAliasPredicate(sql, parsed.whereClause(), parsed.asAliases(), finalVertexAlias, idCol, hasWhere);
                     } else {
                        if (parsed.whereClause().kind() != GremlinSqlTranslator.WhereKind.NEQ_ALIAS) {
                           throw new IllegalArgumentException("Unsupported where() predicate for hop traversal: " + parsed.whereClause().kind());
                        }

                        this.appendNeqAliasPredicate(sql, parsed.whereClause(), parsed.asAliases(), parsed.whereByProperty(), parsed.hops(), mappingConfig, startVertexMapping, idCol, hasWhere);
                     }
                  }

                  hasWhere = true;
               }
            }

            if (parsed.simplePathRequested() && hopCount > 0) {
               StringJoiner cycleConditions = getStringJoiner(hopCount, idCol);

               sql.append(hasWhere ? " AND " : " WHERE ").append(cycleConditions);
            }

            appendLimit(sql, parsed.limit());
            return new TranslationResult(sql.toString(), params);
         }
      }
   }

   @NotNull
   private static StringJoiner getStringJoiner(int hopCount, String idCol) {
      StringJoiner cycleConditions = new StringJoiner(" AND ");

      for(int i = 1; i <= hopCount; ++i) {
         String currentAlias = "v" + i + "." + idCol;
         if (i == 1) {
            cycleConditions.add(currentAlias + " <> v0." + idCol);
         } else {
            StringJoiner priorAliases = new StringJoiner(", ");

            for(int j = 0; j < i; ++j) {
               priorAliases.add("v" + j + "." + idCol);
            }

            cycleConditions.add(currentAlias + " NOT IN (" + priorAliases + ")");
         }
      }
      return cycleConditions;
   }

   private String buildPathSelectClause(List<String> pathByProps, int hopCount, MappingConfig mappingConfig, VertexMapping startVertexMapping, List<HopStep> hops) {
      StringJoiner pathSelect = new StringJoiner(", ");
      List<VertexMapping> hopMappings = new ArrayList<>();
      hopMappings.add(startVertexMapping);
      VertexMapping prev = startVertexMapping;

      for(HopStep hop : hops) {
         if (hop.labels().isEmpty()) {
            hopMappings.add(prev);
         } else {
            EdgeMapping em = this.resolveEdgeMapping(hop.labels().get(0), mappingConfig);
            VertexMapping target = this.resolveHopTargetVertexMapping(mappingConfig, em, "out".equals(hop.direction()), prev);
            hopMappings.add(target);
            prev = target;
         }
      }

      int numProps = pathByProps.size();

      for(int i = 0; i <= hopCount; ++i) {
         String prop = pathByProps.get(Math.min(i, numProps - 1));
         if (i > 0) {
            HopStep prevHop = hops.get(i - 1);
            if ("outE".equals(prevHop.direction()) || "inE".equals(prevHop.direction())) {
               EdgeMapping em = this.resolveEdgeMapping(prevHop.singleLabel(), mappingConfig);
               String edgeCol = em.properties().get(prop);
               if (edgeCol != null) {
                  pathSelect.add("e" + i + "." + edgeCol + " AS " + prop + i);
               } else {
                  pathSelect.add("NULL AS " + prop + i);
               }
               continue;
            }
         }

         VertexMapping vm = hopMappings.get(Math.min(i, hopMappings.size() - 1));
         String mappedColumn = vm.properties().get(prop);
         if (mappedColumn == null) {
            for(VertexMapping candidate : mappingConfig.vertices().values()) {
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

   private VertexMapping resolveHopTargetVertexMapping(MappingConfig mappingConfig, EdgeMapping edgeMapping, boolean outDirection, VertexMapping defaultMapping) {
      VertexMapping resolved = this.resolveTargetVertexMapping(mappingConfig, edgeMapping, outDirection);
      return resolved != null ? resolved : defaultMapping;
   }

   private TranslationResult buildMultiLabelHopUnionSql(ParsedTraversal parsed, MappingConfig mappingConfig, VertexMapping startVertexMapping) {
      int hopCount = parsed.hops().size();
      if (hopCount == 1) {
         HopStep hop = parsed.hops().get(0);
         List<String> labels = hop.labels();
         List<String> branchSqls = new ArrayList<>();
         List<Object> allParams = new ArrayList<>();

         for(String label : labels) {
            List<HopStep> singleHop = List.of(new HopStep(hop.direction(), label));
            ParsedTraversal singleParsed = this.withHops(parsed, singleHop);
            TranslationResult br = this.buildHopTraversalSql(singleParsed, mappingConfig, startVertexMapping);
            branchSqls.add(br.sql());
            allParams.addAll(br.parameters());
         }

         String unionKeyword = parsed.dedupRequested() ? " UNION " : " UNION ALL ";
         StringBuilder finalSql = new StringBuilder(String.join(unionKeyword, branchSqls));
         appendLimit(finalSql, parsed.limit());
         return new TranslationResult(finalSql.toString(), allParams);
      } else {
         List<Object> params = new ArrayList<>();
         StringBuilder sql = new StringBuilder();
         String idCol = startVertexMapping.idColumn();
         String finalVertexAlias = "v" + hopCount;
         if (parsed.countRequested()) {
            sql.append(parsed.dedupRequested() ? "SELECT COUNT(DISTINCT " + finalVertexAlias + "." + idCol + ") AS count" : "SELECT COUNT(*) AS count");
          } else if (!parsed.projections().isEmpty()) {
             // Determine the final vertex mapping after all hops
             VertexMapping finalVertexMapping = startVertexMapping;
             for(HopStep hop : parsed.hops()) {
                if (!hop.labels().isEmpty()) {
                   EdgeMapping em = this.resolveEdgeMapping(hop.labels().get(0), mappingConfig);
                   finalVertexMapping = this.resolveHopTargetVertexMapping(mappingConfig, em, "out".equals(hop.direction()), finalVertexMapping);
                }
             }

             StringJoiner pj = new StringJoiner(", ");

             for(ProjectionField pf : parsed.projections()) {
                if (pf.kind() != GremlinSqlTranslator.ProjectionKind.EDGE_PROPERTY) {
                   throw new IllegalArgumentException("Hop projection currently supports by('property') only");
                }

                pj.add(finalVertexAlias + "." + this.mapVertexProperty(finalVertexMapping, pf.property()) + " AS " + this.quoteAlias(pf.alias()));
             }

             sql.append(parsed.dedupRequested() ? "SELECT DISTINCT " : "SELECT ").append(pj);
         } else if (parsed.pathSeen() && !parsed.pathByProperties().isEmpty()) {
            sql.append(this.buildPathSelectClause(parsed.pathByProperties(), hopCount, mappingConfig, startVertexMapping, parsed.hops()));
         } else if (parsed.valueProperty() != null) {
            VertexMapping lastVm = startVertexMapping;

            for(HopStep hop : parsed.hops()) {
               if (!hop.labels().isEmpty()) {
                  EdgeMapping em = this.resolveEdgeMapping(hop.labels().get(0), mappingConfig);
                  lastVm = this.resolveHopTargetVertexMapping(mappingConfig, em, "out".equals(hop.direction()), lastVm);
               }
            }

            String col = lastVm.properties().get(parsed.valueProperty());
            if (col == null) {
               for(VertexMapping vm : mappingConfig.vertices().values()) {
                  if (vm.properties().containsKey(parsed.valueProperty())) {
                     col = vm.properties().get(parsed.valueProperty());
                     break;
                  }
               }
            }

            if (col == null) {
               throw new IllegalArgumentException("No vertex property mapping found for property: " + parsed.valueProperty());
            }

            sql.append("SELECT ").append(parsed.dedupRequested() ? "DISTINCT " : "").append(finalVertexAlias).append('.').append(col).append(" AS ").append(parsed.valueProperty());
         } else {
            sql.append(parsed.dedupRequested() ? "SELECT DISTINCT " : "SELECT ").append(finalVertexAlias).append(".*");
         }

         boolean useStartSubquery = parsed.preHopLimit() != null;
         if (useStartSubquery) {
            StringBuilder sq = (new StringBuilder("(SELECT * FROM ")).append(startVertexMapping.table());
            List<Object> sqp = new ArrayList<>();
            this.appendWhereClauseForVertex(sq, sqp, parsed.filters(), startVertexMapping);
            if (parsed.whereClause() != null) {
               if (this.isStructuredWherePredicate(parsed.whereClause())) {
                  this.appendStructuredWherePredicate(sq, sqp, parsed.whereClause(), mappingConfig, startVertexMapping, null, !parsed.filters().isEmpty());
               }
            }

            sq.append(" LIMIT ").append(parsed.preHopLimit()).append(")");
            params.addAll(sqp);
            sql.append(" FROM ").append(sq).append(" v0");
         } else {
            sql.append(" FROM ").append(startVertexMapping.table()).append(" v0");
         }

         VertexMapping currentSource = startVertexMapping;

         for(int i = 0; i < hopCount; ++i) {
            HopStep hop = parsed.hops().get(i);
            List<String> labels = hop.labels();
            String dir = hop.direction();
            String toAlias = "v" + (i + 1);
            String fromId = "v" + i + "." + idCol;
            String toId = toAlias + "." + idCol;
            EdgeMapping primaryEdge = this.resolveEdgeMapping(labels.get(0), mappingConfig);
            VertexMapping primaryTarget = this.resolveHopTargetVertexMapping(mappingConfig, primaryEdge, "out".equals(dir), currentSource);
            String eAlias = "e" + (i + 1);
            sql.append(" JOIN ").append(primaryEdge.table()).append(' ').append(eAlias);
            sql.append(" ON ").append(eAlias).append('.').append("out".equals(dir) ? primaryEdge.outColumn() : primaryEdge.inColumn()).append(" = ").append(fromId);
            sql.append(" JOIN ").append(primaryTarget.table()).append(' ').append(toAlias);
            sql.append(" ON ").append(toId).append(" = ").append(eAlias).append('.').append("out".equals(dir) ? primaryEdge.inColumn() : primaryEdge.outColumn());

            for(int l = 1; l < labels.size(); ++l) {
               EdgeMapping altEdge = this.resolveEdgeMapping(labels.get(l), mappingConfig);
               VertexMapping altTarget = this.resolveHopTargetVertexMapping(mappingConfig, altEdge, "out".equals(dir), currentSource);
               String altEAlias = "e" + (i + 1) + "_" + l;
               String altVAlias = "vt" + (i + 1) + "_" + l;
               sql.append(" LEFT JOIN ").append(altEdge.table()).append(' ').append(altEAlias);
               sql.append(" ON ").append(altEAlias).append('.').append("out".equals(dir) ? altEdge.outColumn() : altEdge.inColumn()).append(" = ").append(fromId);
               sql.append(" LEFT JOIN ").append(altTarget.table()).append(' ').append(altVAlias);
               sql.append(" ON ").append(altVAlias).append('.').append(altTarget.idColumn()).append(" = ").append(altEAlias).append('.').append("out".equals(dir) ? altEdge.inColumn() : altEdge.outColumn());
            }

            currentSource = primaryTarget;
         }

         boolean hasWhere = false;
         if (!useStartSubquery) {
            if (!parsed.filters().isEmpty()) {
               this.appendWhereClauseForVertexAlias(sql, params, parsed.filters(), startVertexMapping, "v0");
               hasWhere = true;
            }

            if (parsed.whereClause() != null) {
               if (this.isStructuredWherePredicate(parsed.whereClause())) {
                  this.appendStructuredWherePredicate(sql, params, parsed.whereClause(), mappingConfig, startVertexMapping, "v0", hasWhere);
               }

               hasWhere = true;
            }
         }

         if (parsed.simplePathRequested() && hopCount > 0) {
            StringJoiner cc = getStringJoiner(hopCount, idCol);

            sql.append(hasWhere ? " AND " : " WHERE ").append(cc);
         }

         appendLimit(sql, parsed.limit());
         return new TranslationResult(sql.toString(), params);
      }
   }

   private TranslationResult buildBothHopUnionSql(ParsedTraversal parsed, MappingConfig mappingConfig, VertexMapping startVertexMapping) {
      int hopCount = parsed.hops().size();
      String idCol = startVertexMapping.idColumn();
      String finalVertexAlias = "v" + hopCount;
      String selectClause;
      if (parsed.countRequested()) {
         selectClause = finalVertexAlias + "." + idCol;
       } else if (!parsed.projections().isEmpty()) {
          // Determine the final vertex mapping after all hops
          VertexMapping finalVertexMapping = startVertexMapping;
          for(HopStep hop : parsed.hops()) {
             if (!hop.labels().isEmpty()) {
                EdgeMapping em = this.resolveEdgeMapping(hop.labels().get(0), mappingConfig);
                finalVertexMapping = this.resolveHopTargetVertexMapping(mappingConfig, em, "out".equals(hop.direction()), finalVertexMapping);
             }
          }

          StringJoiner projectionSelect = new StringJoiner(", ");

          for(ProjectionField projection : parsed.projections()) {
             if (projection.kind() != GremlinSqlTranslator.ProjectionKind.EDGE_PROPERTY) {
                throw new IllegalArgumentException("both() projection currently supports by('property') only");
             }

             String mappedColumn = this.mapVertexProperty(finalVertexMapping, projection.property());
             projectionSelect.add(finalVertexAlias + "." + mappedColumn + " AS " + this.quoteAlias(projection.alias()));
          }

          selectClause = projectionSelect.toString();
      } else if (parsed.pathSeen() && !parsed.pathByProperties().isEmpty()) {
         selectClause = this.buildPathSelectClause(parsed.pathByProperties(), hopCount, mappingConfig, startVertexMapping, parsed.hops()).substring("SELECT ".length());
       } else if (parsed.valueProperty() != null) {
          // Determine the final vertex mapping after all hops
          VertexMapping finalVertexMapping = startVertexMapping;
          for(HopStep hop : parsed.hops()) {
             if (!hop.labels().isEmpty()) {
                EdgeMapping em = this.resolveEdgeMapping(hop.labels().get(0), mappingConfig);
                finalVertexMapping = this.resolveHopTargetVertexMapping(mappingConfig, em, "out".equals(hop.direction()), finalVertexMapping);
             }
          }

          String mappedColumn = this.mapVertexProperty(finalVertexMapping, parsed.valueProperty());
          selectClause = finalVertexAlias + "." + mappedColumn + " AS " + parsed.valueProperty();
      } else {
         selectClause = finalVertexAlias + ".*";
      }

      boolean useStartSubquery = parsed.preHopLimit() != null;
      StringBuilder startFrom = new StringBuilder();
      List<Object> sharedStartParams = new ArrayList<>();
      if (useStartSubquery) {
         StringBuilder startSubquery = (new StringBuilder("(SELECT * FROM ")).append(startVertexMapping.table());
         this.appendWhereClauseForVertex(startSubquery, sharedStartParams, parsed.filters(), startVertexMapping);
         if (parsed.whereClause() != null) {
            if (this.isStructuredWherePredicate(parsed.whereClause())) {
               this.appendStructuredWherePredicate(startSubquery, sharedStartParams, parsed.whereClause(), mappingConfig, startVertexMapping, null, !parsed.filters().isEmpty());
            }
         }

         startSubquery.append(" LIMIT ").append(parsed.preHopLimit()).append(")");
         startFrom.append(startSubquery).append(" v0");
      } else {
         startFrom.append(startVertexMapping.table()).append(" v0");
      }

      List<Object> outParams = new ArrayList<>();
      List<Object> inParams = new ArrayList<>();
      StringBuilder outBranch = (new StringBuilder("SELECT ")).append(selectClause).append(" FROM ").append(startFrom);
      StringBuilder inBranch = (new StringBuilder("SELECT ")).append(selectClause).append(" FROM ").append(startFrom);

      for(int i = 0; i < hopCount; ++i) {
         HopStep hop = parsed.hops().get(i);
         EdgeMapping edgeMapping = this.resolveEdgeMapping(hop.singleLabel(), mappingConfig);
         String edgeAlias = "e" + (i + 1);
         String fromVertexAlias = "v" + i;
         String toVertexAlias = "v" + (i + 1);
         String fromId = fromVertexAlias + "." + idCol;
         String toId = toVertexAlias + "." + idCol;
         if ("both".equals(hop.direction())) {
            outBranch.append(" JOIN ").append(edgeMapping.table()).append(" ").append(edgeAlias).append(" ON ").append(edgeAlias).append(".").append(edgeMapping.outColumn()).append(" = ").append(fromId).append(" JOIN ").append(startVertexMapping.table()).append(" ").append(toVertexAlias).append(" ON ").append(toId).append(" = ").append(edgeAlias).append(".").append(edgeMapping.inColumn());
            inBranch.append(" JOIN ").append(edgeMapping.table()).append(" ").append(edgeAlias).append(" ON ").append(edgeAlias).append(".").append(edgeMapping.inColumn()).append(" = ").append(fromId).append(" JOIN ").append(startVertexMapping.table()).append(" ").append(toVertexAlias).append(" ON ").append(toId).append(" = ").append(edgeAlias).append(".").append(edgeMapping.outColumn());
         } else {
            String var10000 = edgeMapping.table();
            String outJoin = " JOIN " + var10000 + " " + edgeAlias + " ON " + edgeAlias + "." + ("out".equals(hop.direction()) ? edgeMapping.outColumn() : edgeMapping.inColumn()) + " = " + fromId + " JOIN " + startVertexMapping.table() + " " + toVertexAlias + " ON " + toId + " = " + edgeAlias + "." + ("out".equals(hop.direction()) ? edgeMapping.inColumn() : edgeMapping.outColumn());
            outBranch.append(outJoin);
            inBranch.append(outJoin);
         }
      }

      if (!useStartSubquery) {
         boolean hasWhere = false;
         if (!parsed.filters().isEmpty()) {
            this.appendWhereClauseForVertexAlias(outBranch, outParams, parsed.filters(), startVertexMapping, "v0");
            this.appendWhereClauseForVertexAlias(inBranch, inParams, parsed.filters(), startVertexMapping, "v0");
            hasWhere = true;
         }

         if (parsed.whereClause() != null && this.isStructuredWherePredicate(parsed.whereClause())) {
            this.appendStructuredWherePredicate(outBranch, outParams, parsed.whereClause(), mappingConfig, startVertexMapping, "v0", hasWhere);
            this.appendStructuredWherePredicate(inBranch, inParams, parsed.whereClause(), mappingConfig, startVertexMapping, "v0", hasWhere);
         }
      }

      if (parsed.simplePathRequested() && hopCount > 0) {
         StringJoiner cycleConditions = getStringJoiner(hopCount, idCol);

         outBranch.append(" WHERE ").append(cycleConditions);
         inBranch.append(" WHERE ").append(cycleConditions);
      }

      String unionKeyword = parsed.dedupRequested() ? " UNION " : " UNION ALL ";
      String unionSql = outBranch + unionKeyword + inBranch;
      StringBuilder finalSql;
      if (parsed.countRequested()) {
         if (parsed.dedupRequested()) {
            finalSql = (new StringBuilder("SELECT COUNT(DISTINCT ")).append(idCol).append(") AS count FROM (").append(unionSql).append(") _u");
         } else {
            finalSql = (new StringBuilder("SELECT COUNT(*) AS count FROM (")).append(unionSql).append(") _u");
         }
      } else if (parsed.dedupRequested()) {
         finalSql = (new StringBuilder("SELECT DISTINCT * FROM (")).append(unionSql).append(") _u");
      } else {
         finalSql = new StringBuilder(unionSql);
      }

      List<Object> params = new ArrayList<>();
      if (useStartSubquery) {
         params.addAll(sharedStartParams);
         params.addAll(sharedStartParams);
      } else {
         params.addAll(outParams);
         params.addAll(inParams);
      }

      appendLimit(finalSql, parsed.limit());
      return new TranslationResult(finalSql.toString(), params);
   }

   private TranslationResult buildEdgeSql(final ParsedTraversal parsed, MappingConfig mappingConfig) {
      String label = parsed.label();
      if (label == null) {
         label = this.resolveEdgeLabelFromFilters(parsed.filters(), mappingConfig);
      }

      EdgeMapping edgeMapping = mappingConfig.edges().get(label);
      if (edgeMapping == null) {
         throw new IllegalArgumentException("No edge mapping found for label: " + label);
      }
      if (!parsed.selectFields().isEmpty()) {
         return this.buildEdgeAliasSelectSql(parsed, mappingConfig, edgeMapping);
      } else if (parsed.groupCountProperty() != null) {
         return this.buildEdgeGroupCountSql(parsed, edgeMapping);
      } else if (!parsed.hops().isEmpty()) {
         return this.buildEdgeToVertexSql(parsed, mappingConfig, edgeMapping);
      } else if (!parsed.projections().isEmpty()) {
         return this.buildEdgeProjectionSql(parsed, mappingConfig, edgeMapping);
      } else {
         Integer limit = parsed.limit() != null ? parsed.limit() : parsed.preHopLimit();
         if (parsed.valueMapRequested()) {
            StringJoiner selectCols = new StringJoiner(", ");
            for (Map.Entry<String, String> entry : edgeMapping.properties().entrySet()) {
               selectCols.add(entry.getValue() + " AS " + this.quoteAlias(entry.getKey()));
            }

            List<Object> params = new ArrayList<>();
            StringBuilder sql = (new StringBuilder("SELECT "))
                  .append(parsed.dedupRequested() ? "DISTINCT " : "")
                  .append(selectCols)
                  .append(" FROM ")
                  .append(edgeMapping.table());
            this.appendWhereClauseForEdge(sql, params, parsed.filters(), edgeMapping);
            appendOrderBy(sql, parsed.orderByProperty(), parsed.orderDirection());
            appendLimit(sql, limit);
            return new TranslationResult(sql.toString(), params);
         } else if (parsed.countRequested() && parsed.sumRequested()) {
            throw new IllegalArgumentException("count() cannot be combined with sum()");
         } else {
            String selectClause = parsed.countRequested() ? "COUNT(*) AS count" : "*";
            List<Object> params = new ArrayList<>();
            if (parsed.sumRequested()) {
               if (parsed.valueProperty() == null) {
                  throw new IllegalArgumentException("sum() requires values('property') before aggregation");
               }

               String mappedColumn = this.mapEdgeProperty(edgeMapping, parsed.valueProperty());
               selectClause = "SUM(" + mappedColumn + ") AS sum";
            }

            if (parsed.meanRequested()) {
               if (parsed.valueProperty() == null) {
                  throw new IllegalArgumentException("mean() requires values('property') before aggregation");
               }

               String mappedColumn = this.mapEdgeProperty(edgeMapping, parsed.valueProperty());
               selectClause = "AVG(" + mappedColumn + ") AS mean";
            }

            if (parsed.valueProperty() != null && !parsed.countRequested() && !parsed.sumRequested() && !parsed.meanRequested()) {
               String mappedColumn = this.mapEdgeProperty(edgeMapping, parsed.valueProperty());
               selectClause = mappedColumn + " AS " + parsed.valueProperty();
            }

            if (parsed.sumRequested()) {
               String mappedColumn = this.mapEdgeProperty(edgeMapping, parsed.valueProperty());
               selectClause = "SUM(" + mappedColumn + ") AS sum";
            }

            if (parsed.meanRequested()) {
               String mappedColumn = this.mapEdgeProperty(edgeMapping, parsed.valueProperty());
               selectClause = "AVG(" + mappedColumn + ") AS mean";
            }

            StringBuilder sql = (new StringBuilder("SELECT ")).append(selectClause).append(" FROM ").append(edgeMapping.table());
            this.appendWhereClauseForEdge(sql, params, parsed.filters(), edgeMapping);
            appendOrderBy(sql, parsed.orderByProperty(), parsed.orderDirection());
            appendLimit(sql, limit);
            return new TranslationResult(sql.toString(), params);
         }
      }
   }

   private TranslationResult buildEdgeToVertexSql(ParsedTraversal parsed, MappingConfig mappingConfig, EdgeMapping edgeMapping) {
      if (parsed.hops().size() != 1) {
         throw new IllegalArgumentException("Only a single outV(), inV(), or bothV() hop is supported after g.E()");
      } else {
         HopStep hop = parsed.hops().get(0);
         if (!"outV".equals(hop.direction()) && !"inV".equals(hop.direction()) && !"bothV".equals(hop.direction())) {
            throw new IllegalArgumentException("Only outV(), inV(), or bothV() hops are supported after g.E()");
         } else {
            VertexMapping outVertexMapping = this.resolveTargetVertexMapping(mappingConfig, edgeMapping, false);
            VertexMapping inVertexMapping = this.resolveTargetVertexMapping(mappingConfig, edgeMapping, true);
            VertexMapping fallbackVertexMapping = this.resolveVertexMappingForEdgeProjection(mappingConfig);
            if (outVertexMapping == null) {
               outVertexMapping = fallbackVertexMapping;
            }
            if (inVertexMapping == null) {
               inVertexMapping = fallbackVertexMapping;
            }
            if (outVertexMapping == null || inVertexMapping == null) {
               throw new IllegalArgumentException("outV()/inV()/bothV() traversal requires resolvable endpoint vertex mappings");
            }
            Integer limit = parsed.limit() != null ? parsed.limit() : parsed.preHopLimit();
            if ("bothV".equals(hop.direction())) {
               List<Object> outParams = new ArrayList<>();
               String outSelectClause = this.buildEdgeEndpointSelectClause(parsed, outVertexMapping);
               StringBuilder outSql = (new StringBuilder("SELECT ")).append(outSelectClause).append(" FROM ").append(outVertexMapping.table()).append(" v").append(" JOIN ").append(edgeMapping.table()).append(" e").append(" ON v.").append(outVertexMapping.idColumn()).append(" = e.").append(edgeMapping.outColumn());
               this.appendWhereClauseForEdgeAlias(outSql, outParams, parsed.filters(), edgeMapping, "e");

               List<Object> inParams = new ArrayList<>();
               String inSelectClause = this.buildEdgeEndpointSelectClause(parsed, inVertexMapping);
               StringBuilder inSql = (new StringBuilder("SELECT ")).append(inSelectClause).append(" FROM ").append(inVertexMapping.table()).append(" v").append(" JOIN ").append(edgeMapping.table()).append(" e").append(" ON v.").append(inVertexMapping.idColumn()).append(" = e.").append(edgeMapping.inColumn());
               this.appendWhereClauseForEdgeAlias(inSql, inParams, parsed.filters(), edgeMapping, "e");

               StringBuilder union = new StringBuilder("SELECT DISTINCT * FROM (").append(outSql).append(" UNION ALL ").append(inSql).append(") _uv");
               appendOrderBy(union, parsed.orderByProperty(), parsed.orderDirection());
               appendLimit(union, limit);
               List<Object> params = new ArrayList<>();
               params.addAll(outParams);
               params.addAll(inParams);
               return new TranslationResult(union.toString(), params);
            }

            boolean isOutEndpoint = "outV".equals(hop.direction());
            VertexMapping endpointMapping = isOutEndpoint ? outVertexMapping : inVertexMapping;
            String joinColumn = isOutEndpoint ? edgeMapping.outColumn() : edgeMapping.inColumn();
            List<Object> params = new ArrayList<>();
            String selectClause = this.buildEdgeEndpointSelectClause(parsed, endpointMapping);
            StringBuilder sql = (new StringBuilder("SELECT DISTINCT ")).append(selectClause).append(" FROM ").append(endpointMapping.table()).append(" v").append(" JOIN ").append(edgeMapping.table()).append(" e").append(" ON v.").append(endpointMapping.idColumn()).append(" = e.").append(joinColumn);
            this.appendWhereClauseForEdgeAlias(sql, params, parsed.filters(), edgeMapping, "e");
            appendOrderBy(sql, parsed.orderByProperty(), parsed.orderDirection());
            appendLimit(sql, limit);
            return new TranslationResult(sql.toString(), params);
         }
      }
   }

   private String buildEdgeEndpointSelectClause(ParsedTraversal parsed, VertexMapping endpointMapping) {
      if (!parsed.projections().isEmpty()) {
         StringJoiner projectionSelect = new StringJoiner(", ");
         for (ProjectionField projection : parsed.projections()) {
            String alias = this.quoteAlias(projection.alias());
            if (projection.kind() == GremlinSqlTranslator.ProjectionKind.IDENTITY) {
               projectionSelect.add("v" + "." + endpointMapping.idColumn() + " AS " + alias);
               continue;
            }
            if (projection.kind() != GremlinSqlTranslator.ProjectionKind.EDGE_PROPERTY) {
               throw new IllegalArgumentException("outV()/inV()/bothV() projection supports by('property') and by(identity()) only");
            }
            String column = this.mapVertexProperty(endpointMapping, projection.property());
            projectionSelect.add("v" + "." + column + " AS " + alias);
         }
         return projectionSelect.toString();
      }
      if (parsed.valueMapRequested()) {
         return this.buildAliasValueMapSelectForVertex(endpointMapping, "v");
      }
      if (parsed.valueProperty() != null) {
         String column = this.mapVertexProperty(endpointMapping, parsed.valueProperty());
         return "v" + "." + column + " AS " + parsed.valueProperty();
      }
      return "v" + ".*";
   }

   private String buildAliasValueMapSelectForVertex(VertexMapping vertexMapping, String alias) {
      StringJoiner selectCols = new StringJoiner(", ");
      for (Map.Entry<String, String> entry : vertexMapping.properties().entrySet()) {
         selectCols.add(alias + "." + entry.getValue() + " AS " + this.quoteAlias(entry.getKey()));
      }
      return selectCols.toString();
   }

   private String buildAliasValueMapSelectForEdge(EdgeMapping edgeMapping, String alias) {
      StringJoiner selectCols = new StringJoiner(", ");
      for (Map.Entry<String, String> entry : edgeMapping.properties().entrySet()) {
         selectCols.add(alias + "." + entry.getValue() + " AS " + this.quoteAlias(entry.getKey()));
      }
      return selectCols.toString();
   }

   private String resolveEdgeLabelFromFilters(List<HasFilter> filters, MappingConfig mappingConfig) {
      if (mappingConfig.edges().size() == 1) {
         return mappingConfig.edges().keySet().iterator().next();
      } else if (filters.isEmpty()) {
         return this.resolveSingleLabel(mappingConfig.edges(), "edge");
      } else {
         List<String> candidates = new ArrayList<>();

         for(Map.Entry<String, EdgeMapping> entry : mappingConfig.edges().entrySet()) {
            EdgeMapping em = entry.getValue();
            boolean allMatch = filters.stream().allMatch((f) -> "id".equals(f.property()) || "outV".equals(f.property()) || "inV".equals(f.property()) || em.properties().containsKey(f.property()));
            if (allMatch) {
               candidates.add(entry.getKey());
            }
         }

         if (candidates.size() == 1) {
            return candidates.get(0);
         } else {
            return this.resolveSingleLabel(mappingConfig.edges(), "edge");
         }
      }
   }

   private TranslationResult buildEdgeAliasSelectSql(ParsedTraversal parsed, MappingConfig mappingConfig, EdgeMapping rootEdgeMapping) {
      if (parsed.countRequested()) {
         throw new IllegalArgumentException("count() cannot be combined with select(...)");
      } else if (parsed.valueProperty() != null) {
         throw new IllegalArgumentException("values() cannot be combined with select(...)");
      } else if (parsed.whereClause() != null && parsed.whereClause().kind() != GremlinSqlTranslator.WhereKind.NEQ_ALIAS) {
         throw new IllegalArgumentException("Only where('a',neq('b')) is supported with select(...)");
      } else if (parsed.hops().size() == 3 && "outV".equals(parsed.hops().get(0).direction()) && ("outE".equals(parsed.hops().get(1).direction()) || "inE".equals(parsed.hops().get(1).direction())) && ("inV".equals(parsed.hops().get(2).direction()) || "outV".equals(parsed.hops().get(2).direction()))) {
         VertexMapping vertexMapping = Optional.ofNullable(this.resolveVertexMappingForEdgeProjection(mappingConfig)).orElseThrow(() -> new IllegalArgumentException("select(...).by('prop') from g.E() requires exactly one vertex mapping"));
         Map<String, Integer> aliasHopIndex = new HashMap<>();

         for(AsAlias asAlias : parsed.asAliases()) {
            aliasHopIndex.put(asAlias.label(), asAlias.hopIndexAfter());
         }

         String edge2Label = parsed.hops().get(1).singleLabel();
         EdgeMapping secondEdgeMapping = this.resolveEdgeMapping(edge2Label, mappingConfig);
         List<Object> params = new ArrayList<>();
         StringJoiner selectJoiner = new StringJoiner(", ");
         Iterator<SelectField> var10 = parsed.selectFields().iterator();

         while(true) {
            if (var10.hasNext()) {
               SelectField field = var10.next();
               Integer hopIndex = aliasHopIndex.get(field.alias);
               if (hopIndex == null) {
                  throw new IllegalArgumentException("select alias not found: " + field.alias);
               }

               if (field.property() != null && !field.property().isBlank()) {
                  String vertexAlias = "v" + hopIndex;
                  String column = this.mapVertexProperty(vertexMapping, field.property());
                  selectJoiner.add(vertexAlias + "." + column + " AS " + this.quoteAlias(field.alias));
                  continue;
               }

               throw new IllegalArgumentException("select(...).by(...) requires a property for alias: " + field.alias);
            }

            StringBuilder sql = (new StringBuilder("SELECT DISTINCT ")).append(selectJoiner).append(" FROM ").append(rootEdgeMapping.table()).append(" e0").append(" JOIN ").append(vertexMapping.table()).append(" v1").append(" ON v1.").append(vertexMapping.idColumn()).append(" = e0.").append(rootEdgeMapping.outColumn()).append(" JOIN ").append(secondEdgeMapping.table()).append(" e2");
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
               String var10001 = this.mapEdgeFilterProperty(rootEdgeMapping, first.property());
               whereJoiner.add("e0." + var10001 + " = ?");
               params.add(first.value());

               for(int i = 1; i < edgeFilters.size(); ++i) {
                  HasFilter filter = edgeFilters.get(i);
                  var10001 = this.mapEdgeFilterProperty(secondEdgeMapping, filter.property());
                  whereJoiner.add("e2." + var10001 + " = ?");
                  params.add(filter.value());
               }
            }

            if (parsed.whereClause() != null) {
               String leftVertexAlias = this.resolveVertexAliasForWhere(parsed.whereClause().left(), aliasHopIndex);
               String rightVertexAlias = this.resolveVertexAliasForWhere(parsed.whereClause().right(), aliasHopIndex);
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
      } else {
         throw new IllegalArgumentException("select(...).by(...) from g.E() currently supports outV().outE()/inE().inV()/outV() pattern");
      }
   }

   private TranslationResult buildEdgeProjectionSql(ParsedTraversal parsed, MappingConfig mappingConfig, EdgeMapping edgeMapping) {
      if (parsed.countRequested()) {
         throw new IllegalArgumentException("count() cannot be combined with project(...)");
      } else if (parsed.valueProperty() != null) {
         throw new IllegalArgumentException("values() cannot be combined with project(...)");
      } else {
         List<Object> params = new ArrayList<>();
         StringJoiner selectJoiner = new StringJoiner(", ");
         boolean needsOutJoin = false;
         boolean needsInJoin = false;
         VertexMapping outVertexMapping = this.resolveHopTargetVertexMapping(mappingConfig, edgeMapping, false, null);
         VertexMapping inVertexMapping = this.resolveHopTargetVertexMapping(mappingConfig, edgeMapping, true, null);
         if (outVertexMapping == null || inVertexMapping == null) {
            List<String> outProps = new ArrayList<>();
            List<String> inProps = new ArrayList<>();

            for(ProjectionField pf : parsed.projections()) {
               if (pf.kind() == GremlinSqlTranslator.ProjectionKind.OUT_VERTEX_PROPERTY) {
                  outProps.add(pf.property());
               }

               if (pf.kind() == GremlinSqlTranslator.ProjectionKind.IN_VERTEX_PROPERTY) {
                  inProps.add(pf.property());
               }
            }

            VertexMapping fallback = this.resolveVertexMappingForEdgeProjection(mappingConfig);
            if (outVertexMapping == null) {
               outVertexMapping = this.resolveVertexMappingByProperties(mappingConfig, outProps);
               if (outVertexMapping == null) {
                  outVertexMapping = fallback;
               }
            }

            if (inVertexMapping == null) {
               inVertexMapping = this.resolveVertexMappingByProperties(mappingConfig, inProps);
               if (inVertexMapping == null) {
                  inVertexMapping = fallback;
               }
            }

            if (outVertexMapping == null || inVertexMapping == null) {
               throw new IllegalArgumentException("project(...).by(outV()/inV()) requires exactly one vertex mapping or a resolvable vertex label");
            }
         }

         for(ProjectionField projection : parsed.projections()) {
            String alias = this.quoteAlias(projection.alias());
            if (projection.kind() == GremlinSqlTranslator.ProjectionKind.IDENTITY) {
               selectJoiner.add("e." + edgeMapping.idColumn() + " AS " + alias);
            } else if (projection.kind() == GremlinSqlTranslator.ProjectionKind.EDGE_PROPERTY) {
               String column = this.mapEdgeProperty(edgeMapping, projection.property());
               selectJoiner.add("e." + column + " AS " + alias);
            } else if (projection.kind() == GremlinSqlTranslator.ProjectionKind.OUT_VERTEX_PROPERTY) {
               String column = this.mapVertexProperty(outVertexMapping, projection.property());
               selectJoiner.add("ov." + column + " AS " + alias);
               needsOutJoin = true;
            } else if (projection.kind() == GremlinSqlTranslator.ProjectionKind.IN_VERTEX_PROPERTY) {
               String column = this.mapVertexProperty(inVertexMapping, projection.property());
               selectJoiner.add("iv." + column + " AS " + alias);
               needsInJoin = true;
            }
         }

         StringBuilder sql = (new StringBuilder("SELECT ")).append(selectJoiner).append(" FROM ").append(edgeMapping.table()).append(" e");
         if (needsOutJoin) {
            sql.append(" JOIN ").append(outVertexMapping.table()).append(" ov").append(" ON ov.").append(outVertexMapping.idColumn()).append(" = e.").append(edgeMapping.outColumn());
         }

         if (needsInJoin) {
            sql.append(" JOIN ").append(inVertexMapping.table()).append(" iv").append(" ON iv.").append(inVertexMapping.idColumn()).append(" = e.").append(edgeMapping.inColumn());
         }

         this.appendWhereClauseForEdgeAlias(sql, params, parsed.filters(), edgeMapping, "e");
         appendLimit(sql, parsed.limit() != null ? parsed.limit() : parsed.preHopLimit());
         return new TranslationResult(sql.toString(), params);
      }
   }

   private TranslationResult buildVertexProjectionSql(ParsedTraversal parsed, MappingConfig mappingConfig, VertexMapping vertexMapping) {
      if (parsed.countRequested()) {
         throw new IllegalArgumentException("count() cannot be combined with project(...)");
      } else if (parsed.valueProperty() != null) {
         throw new IllegalArgumentException("values() cannot be combined with project(...)");
      } else {
         List<Object> params = new ArrayList<>();
         StringJoiner selectJoiner = new StringJoiner(", ");
         int neighborJoinIdx = 0;

         for(ProjectionField projection : parsed.projections()) {
            String alias = this.quoteAlias(projection.alias());
            if (projection.kind() == GremlinSqlTranslator.ProjectionKind.IDENTITY) {
               selectJoiner.add("v." + vertexMapping.idColumn() + " AS " + alias);
               continue;
            }
            if (projection.kind() != GremlinSqlTranslator.ProjectionKind.EDGE_PROPERTY) {
               if (projection.kind() == GremlinSqlTranslator.ProjectionKind.CHOOSE_VALUES_IS_CONSTANT) {
                  ChooseProjectionSpec chooseSpec = this.parseChooseProjection(projection.property());
                  if (chooseSpec == null) {
                     throw new IllegalArgumentException("Unsupported by() projection expression: " + projection.property());
                  }

                  String column = this.mapVertexProperty(vertexMapping, chooseSpec.property());
                  String operator = this.toSqlOperator(chooseSpec.predicate());
                  String threshold = this.sanitizeNumericLiteral(chooseSpec.predicateValue());
                  String whenTrue = this.toSqlConstantLiteral(chooseSpec.trueConstant());
                  String whenFalse = this.toSqlConstantLiteral(chooseSpec.falseConstant());
                  selectJoiner.add("CASE WHEN v." + column + " " + operator + " " + threshold + " THEN " + whenTrue + " ELSE " + whenFalse + " END AS " + alias);
                  continue;
               }

               if (projection.kind() != GremlinSqlTranslator.ProjectionKind.EDGE_DEGREE) {
                  if (projection.kind() == GremlinSqlTranslator.ProjectionKind.OUT_VERTEX_COUNT || projection.kind() == GremlinSqlTranslator.ProjectionKind.IN_VERTEX_COUNT) {
                     String[] parts = projection.property().split(":", -1);
                     if (parts.length < 2) {
                        throw new IllegalArgumentException("Invalid VERTEX_COUNT property: " + projection.property());
                     }

                     String direction = parts[0];
                     String edgeLabel = parts[1];
                     EdgeMapping edgeMapping = Optional.ofNullable(mappingConfig.edges().get(edgeLabel)).orElseThrow(() -> new IllegalArgumentException("No edge mapping found for label: " + edgeLabel));
                     String anchorCol = "out".equals(direction) ? edgeMapping.outColumn() : edgeMapping.inColumn();
                     String targetCol = "out".equals(direction) ? edgeMapping.inColumn() : edgeMapping.outColumn();
                     VertexMapping targetVertexMapping = this.resolveTargetVertexMapping(mappingConfig, edgeMapping, "out".equals(direction));
                     if (targetVertexMapping == null) {
                         String var56 = "(SELECT COUNT(*) FROM " + edgeMapping.table() + " WHERE " + anchorCol + " = v." + vertexMapping.idColumn() + ")";
                        selectJoiner.add(var56 + " AS " + alias);
                     } else {
                        StringBuilder subq = (new StringBuilder("(SELECT COUNT(*) FROM ")).append(edgeMapping.table()).append(" _e").append(" JOIN ").append(targetVertexMapping.table()).append(" _tv").append(" ON _tv.").append(targetVertexMapping.idColumn()).append(" = _e.").append(targetCol).append(" WHERE _e.").append(anchorCol).append(" = v.").append(vertexMapping.idColumn());

                        for(int i = 2; i < parts.length; ++i) {
                           int eq = parts[i].indexOf(61);
                           if (eq >= 1) {
                              String filterProp = parts[i].substring(0, eq);
                              String filterVal = parts[i].substring(eq + 1);
                              String filterCol = this.mapVertexProperty(targetVertexMapping, filterProp);
                              subq.append(" AND _tv.").append(filterCol).append(" = '").append(filterVal.replace("'", "''")).append("'");
                           }
                        }

                        subq.append(")");
                        String var55 = String.valueOf(subq);
                        selectJoiner.add(var55 + " AS " + alias);
                     }
                  } else if (projection.kind() == GremlinSqlTranslator.ProjectionKind.OUT_NEIGHBOR_PROPERTY || projection.kind() == GremlinSqlTranslator.ProjectionKind.IN_NEIGHBOR_PROPERTY) {
                     String[] parts = projection.property().split(":", 3);
                     if (parts.length < 3) {
                        throw new IllegalArgumentException("Invalid NEIGHBOR_PROPERTY descriptor: " + projection.property());
                     }

                     boolean isOut = "out".equals(parts[0]);
                     String edgeLabel = parts[1];
                     String neighborProp = parts[2];
                     EdgeMapping edgeMapping = Optional.ofNullable(mappingConfig.edges().get(edgeLabel)).orElseThrow(() -> new IllegalArgumentException("No edge mapping found for label: " + edgeLabel));
                     VertexMapping neighborMapping = this.resolveTargetVertexMapping(mappingConfig, edgeMapping, isOut);
                     if (neighborMapping == null || !neighborMapping.properties().containsKey(neighborProp)) {
                        VertexMapping propertyMatched = this.resolveUniqueVertexMappingByProperty(mappingConfig, neighborProp);
                        if (propertyMatched != null) {
                           neighborMapping = propertyMatched;
                        }
                     }
                     if (neighborMapping == null) {
                        neighborMapping = vertexMapping;
                     }

                     String anchorCol = isOut ? edgeMapping.outColumn() : edgeMapping.inColumn();
                     String targetCol = isOut ? edgeMapping.inColumn() : edgeMapping.outColumn();
                     String mappedCol = this.mapVertexProperty(neighborMapping, neighborProp);
                     String subqAlias = "_nje" + neighborJoinIdx;
                     String vAlias = "_njv" + neighborJoinIdx;
                     ++neighborJoinIdx;
                     String subq = "(SELECT " + this.dialect.stringAgg(vAlias + "." + mappedCol, ",") + " FROM " + edgeMapping.table() + " " + subqAlias + " JOIN " + neighborMapping.table() + " " + vAlias + " ON " + vAlias + "." + neighborMapping.idColumn() + " = " + subqAlias + "." + targetCol + " WHERE " + subqAlias + "." + anchorCol + " = v." + vertexMapping.idColumn() + ")";
                     selectJoiner.add(subq + " AS " + alias);
                  }
               } else {
                  String[] parts = projection.property().split(":", -1);
                  if (parts.length < 2) {
                     throw new IllegalArgumentException("Invalid EDGE_DEGREE property: " + projection.property());
                  }

                  String direction = parts[0];
                  String edgeLabel = parts[1];
                  EdgeMapping edgeMapping = Optional.ofNullable(mappingConfig.edges().get(edgeLabel)).orElseThrow(() -> new IllegalArgumentException("No edge mapping found for label: " + edgeLabel));
                  String joinColumn = "out".equals(direction) ? edgeMapping.outColumn() : edgeMapping.inColumn();
                  StringBuilder subquery = (new StringBuilder("(SELECT COUNT(*) FROM ")).append(edgeMapping.table()).append(" WHERE ").append(joinColumn).append(" = v.").append(vertexMapping.idColumn());

                  for(int i = 2; i < parts.length; ++i) {
                     int eq = parts[i].indexOf(61);
                     if (eq >= 1) {
                        String filterProp = parts[i].substring(0, eq);
                        String filterVal = parts[i].substring(eq + 1);
                        String filterCol = this.mapEdgeProperty(edgeMapping, filterProp);
                        subquery.append(" AND ").append(filterCol).append(" = '").append(filterVal.replace("'", "''")).append("'");
                     }
                  }

                  subquery.append(")");
                  String var10001 = String.valueOf(subquery);
                  selectJoiner.add(var10001 + " AS " + alias);
               }
            } else {
               String column = this.mapVertexProperty(vertexMapping, projection.property());
               selectJoiner.add("v." + column + " AS " + alias);
            }
         }

         StringBuilder baseSql = (new StringBuilder(parsed.dedupRequested() ? "SELECT DISTINCT " : "SELECT ")).append(selectJoiner).append(" FROM ").append(vertexMapping.table()).append(" v");
         this.appendWhereClauseForVertexAlias(baseSql, params, parsed.filters(), vertexMapping, "v");
         if (parsed.whereClause() != null && this.isStructuredWherePredicate(parsed.whereClause())) {
            this.appendStructuredWherePredicate(baseSql, params, parsed.whereClause(), mappingConfig, vertexMapping, "v", !parsed.filters().isEmpty());
         }

         Integer i = parsed.limit() != null ? parsed.limit() : parsed.preHopLimit();
         if (parsed.whereClause() != null) {
            if (parsed.whereClause().kind() != GremlinSqlTranslator.WhereKind.PROJECT_GT && parsed.whereClause().kind() != GremlinSqlTranslator.WhereKind.PROJECT_GTE) {
               if (!this.isStructuredWherePredicate(parsed.whereClause())) {
                  throw new IllegalArgumentException("Only where(select('alias').is(gt/gte(n))) or where(outE/inE/bothE(...)) or where(out/in('label').has(...)) is supported with project(...)");
               } else {
                  appendOrderBy(baseSql, parsed.orderByProperty(), parsed.orderDirection());
                  appendLimit(baseSql, i);
                  return new TranslationResult(baseSql.toString(), params);
               }
            } else {
               String numericLiteral = this.sanitizeNumericLiteral(parsed.whereClause().right());
               String operator = parsed.whereClause().kind() == GremlinSqlTranslator.WhereKind.PROJECT_GTE ? ">=" : ">";
               StringBuilder wrapped = (new StringBuilder("SELECT * FROM (")).append(baseSql).append(") p WHERE p.").append(this.quoteAlias(parsed.whereClause().left())).append(" ").append(operator).append(" ").append(numericLiteral);
               appendOrderBy(wrapped, parsed.orderByProperty(), parsed.orderDirection());
               appendLimit(wrapped, i);
               return new TranslationResult(wrapped.toString(), params);
            }
         } else {
            appendOrderBy(baseSql, parsed.orderByProperty(), parsed.orderDirection());
            appendLimit(baseSql, i);
            return new TranslationResult(baseSql.toString(), params);
         }
      }
   }

   private String mapEdgeFilterProperty(EdgeMapping mapping, String property) {
      if ("id".equals(property)) {
         return mapping.idColumn();
      } else if ("outV".equals(property)) {
         return mapping.outColumn();
      } else {
         return "inV".equals(property) ? mapping.inColumn() : this.mapEdgeProperty(mapping, property);
      }
   }

   private String resolveVertexAliasForWhere(String aliasLabel, Map<String, Integer> aliasHopIndex) {
      Integer hopIndex = aliasHopIndex.get(aliasLabel);
      if (hopIndex == null) {
         throw new IllegalArgumentException("where() alias not found: " + aliasLabel);
      } else {
         return "v" + hopIndex;
      }
   }

   private String sanitizeNumericLiteral(String raw) {
      String value = raw == null ? "" : raw.trim();
      if (!value.matches("-?\\d+(\\.\\d+)?")) {
         throw new IllegalArgumentException("where(...).is(gt/gte(...)) expects a numeric literal");
      } else {
         return value;
      }
   }

   private ChooseProjectionSpec parseChooseProjection(String expression) {
      if (expression == null || expression.isBlank()) {
         return null;
      } else {
         Matcher matcher = CHOOSE_VALUES_IS_CONSTANT_PATTERN.matcher(expression.trim());
         return matcher.matches() ? new ChooseProjectionSpec(this.unquote(matcher.group(1)), matcher.group(2), matcher.group(3).trim(), matcher.group(4).trim(), matcher.group(5).trim()) : null;
      }
   }

   private String toSqlOperator(String predicate) {
      return switch (predicate) {
         case "gt" -> ">";
         case "gte" -> ">=";
         case "lt" -> "<";
         case "lte" -> "<=";
         case "eq" -> "=";
         case "neq" -> "!=";
         default -> throw new IllegalArgumentException("Unsupported choose() predicate: " + predicate);
      };
   }

   private String toSqlConstantLiteral(String rawConstant) {
      String trimmed = rawConstant == null ? "" : rawConstant.trim();
      if (trimmed.isEmpty()) {
         throw new IllegalArgumentException("choose(...).constant(...) must not be empty");
      } else if ((trimmed.startsWith("\"") && trimmed.endsWith("\"")) || (trimmed.startsWith("'") && trimmed.endsWith("'"))) {
         return "'" + this.unquote(trimmed).replace("'", "''") + "'";
      } else if (trimmed.matches("-?\\d+(\\.\\d+)?")) {
         return this.sanitizeNumericLiteral(trimmed);
      } else if ("true".equalsIgnoreCase(trimmed) || "false".equalsIgnoreCase(trimmed)) {
         return trimmed.toUpperCase();
      } else {
         throw new IllegalArgumentException("choose(...,constant(x),constant(y)) supports quoted string, numeric, or boolean constants only");
      }
   }

   private boolean isStructuredWherePredicate(WhereClause whereClause) {
      return whereClause != null && switch (whereClause.kind()) {
         case EDGE_EXISTS, OUT_NEIGHBOR_HAS, IN_NEIGHBOR_HAS, AND, OR, NOT -> true;
         default -> false;
      };
   }

   private void appendStructuredWherePredicate(StringBuilder sql, List<Object> params, WhereClause whereClause, MappingConfig mappingConfig, VertexMapping vertexMapping, String vertexAlias, boolean hasExistingWhere) {
      String predicate = this.buildStructuredWherePredicateSql(whereClause, mappingConfig, vertexMapping, vertexAlias, params);
      sql.append(hasExistingWhere ? " AND " : " WHERE ").append(predicate);
   }

   private String buildStructuredWherePredicateSql(WhereClause whereClause, MappingConfig mappingConfig, VertexMapping vertexMapping, String vertexAlias, List<Object> params) {
      return switch (whereClause.kind()) {
         case EDGE_EXISTS -> this.buildEdgeExistsPredicateSql(whereClause, mappingConfig, vertexMapping, vertexAlias, params);
         case OUT_NEIGHBOR_HAS, IN_NEIGHBOR_HAS -> this.buildNeighborHasPredicateSql(whereClause, mappingConfig, vertexMapping, vertexAlias, params);
         case AND -> this.joinStructuredPredicates(whereClause.clauses(), " AND ", mappingConfig, vertexMapping, vertexAlias, params);
         case OR -> this.joinStructuredPredicates(whereClause.clauses(), " OR ", mappingConfig, vertexMapping, vertexAlias, params);
         case NOT -> {
            if (whereClause.clauses().size() != 1) {
               throw new IllegalArgumentException("where(not(...)) requires exactly one inner predicate");
            }
            yield "NOT (" + this.buildStructuredWherePredicateSql(whereClause.clauses().get(0), mappingConfig, vertexMapping, vertexAlias, params) + ")";
         }
         default -> throw new IllegalArgumentException("Unsupported structured where() predicate: " + whereClause.kind());
      };
   }

   private String joinStructuredPredicates(List<WhereClause> clauses, String delimiter, MappingConfig mappingConfig, VertexMapping vertexMapping, String vertexAlias, List<Object> params) {
      if (clauses == null || clauses.isEmpty()) {
         throw new IllegalArgumentException("Compound where() predicate requires inner predicates");
      }
      StringJoiner joiner = new StringJoiner(delimiter);
      for (WhereClause clause : clauses) {
         joiner.add("(" + this.buildStructuredWherePredicateSql(clause, mappingConfig, vertexMapping, vertexAlias, params) + ")");
      }
      return "(" + joiner + ")";
   }

    private String buildEdgeExistsPredicateSql(WhereClause whereClause, MappingConfig mappingConfig, VertexMapping vertexMapping, String vertexAlias, List<Object> params) {
       EdgeMapping edgeMapping = this.resolveEdgeMapping(whereClause.right(), mappingConfig);
       String edgeAlias = "we";
       String idRef = vertexAlias == null ? vertexMapping.idColumn() : vertexAlias + "." + vertexMapping.idColumn();
       String correlation;
       if ("outE".equals(whereClause.left())) {
          correlation = edgeAlias + "." + edgeMapping.outColumn() + " = " + idRef;
       } else if ("inE".equals(whereClause.left())) {
          correlation = edgeAlias + "." + edgeMapping.inColumn() + " = " + idRef;
       } else {
          correlation = "(" + edgeAlias + "." + edgeMapping.outColumn() + " = " + idRef + " OR " + edgeAlias + "." + edgeMapping.inColumn() + " = " + idRef + ")";
       }

       // Check if this is a count comparison like outE('LABEL').count().is(0) or outE('LABEL').count().is(gt(5))
       HasFilter countFilter = whereClause.filters().stream().filter(f -> "count".equals(f.property())).findFirst().orElse(null);
       if (countFilter != null) {
          // For count comparisons: COUNT(*) operator value
          StringBuilder countSql = new StringBuilder("(SELECT COUNT(*) FROM ");
          countSql.append(edgeMapping.table()).append(' ').append(edgeAlias).append(" WHERE ").append(correlation);
          
          // Add any non-count filters
          for (HasFilter filter : whereClause.filters()) {
             if (!"count".equals(filter.property())) {
                String column = this.mapEdgeFilterProperty(edgeMapping, filter.property());
                if ("IS NULL".equals(filter.operator())) {
                   countSql.append(" AND ").append(edgeAlias).append('.').append(column).append(" IS NULL");
                } else {
                   countSql.append(" AND ").append(edgeAlias).append('.').append(column).append(" ").append(filter.operator()).append(" ?");
                   params.add(filter.value());
                }
             }
          }
          countSql.append(") ").append(this.toSqlOperator(countFilter.operator())).append(" ?");
          // COUNT(*) returns an integer — bind the threshold as Integer to avoid type-mismatch
          // in strict JDBC drivers (e.g. H2) when comparing INTEGER to VARCHAR '0'.
          try {
             params.add(Integer.parseInt(countFilter.value()));
          } catch (NumberFormatException e) {
             params.add(countFilter.value());
          }
          return countSql.toString();
       }

       // Standard EXISTS clause
       StringBuilder existsSql = (new StringBuilder("EXISTS (SELECT 1 FROM ")).append(edgeMapping.table()).append(' ').append(edgeAlias).append(" WHERE ").append(correlation);

       for (HasFilter filter : whereClause.filters()) {
          String column = this.mapEdgeFilterProperty(edgeMapping, filter.property());
          if ("IS NULL".equals(filter.operator())) {
             existsSql.append(" AND ").append(edgeAlias).append('.').append(column).append(" IS NULL");
          } else {
             existsSql.append(" AND ").append(edgeAlias).append('.').append(column).append(" ").append(filter.operator()).append(" ?");
             params.add(filter.value());
          }
       }
       return existsSql.append(")").toString();
    }

   private String buildNeighborHasPredicateSql(WhereClause whereClause, MappingConfig mappingConfig, VertexMapping vertexMapping, String vertexAlias, List<Object> params) {
      EdgeMapping edgeMapping = this.resolveEdgeMapping(whereClause.right(), mappingConfig);
      boolean isOut = "out".equals(whereClause.left());
      String anchorCol = isOut ? edgeMapping.outColumn() : edgeMapping.inColumn();
      String targetCol = isOut ? edgeMapping.inColumn() : edgeMapping.outColumn();
      VertexMapping neighborVertexMapping = this.resolveTargetVertexMapping(mappingConfig, edgeMapping, isOut);
      boolean missingFilterProps = false;
      if (neighborVertexMapping != null) {
         for(HasFilter filter : whereClause.filters()) {
            if (!neighborVertexMapping.properties().containsKey(filter.property())) {
               missingFilterProps = true;
               break;
            }
         }
      }

      if (neighborVertexMapping == null || missingFilterProps) {
         List<String> filterProps = whereClause.filters().stream().map(HasFilter::property).toList();
         VertexMapping propMatched = this.resolveVertexMappingByProperties(mappingConfig, filterProps);
         if (propMatched != null) {
            neighborVertexMapping = propMatched;
         }
      }
      if (neighborVertexMapping == null) {
         neighborVertexMapping = vertexMapping;
      }

      String idRef = vertexAlias == null ? vertexMapping.idColumn() : vertexAlias + "." + vertexMapping.idColumn();
      StringBuilder existsSql = (new StringBuilder("EXISTS (SELECT 1 FROM ")).append(edgeMapping.table()).append(" _we").append(" JOIN ").append(neighborVertexMapping.table()).append(" _wv").append(" ON _wv.").append(neighborVertexMapping.idColumn()).append(" = _we.").append(targetCol).append(" WHERE _we.").append(anchorCol).append(" = ").append(idRef);

      for(HasFilter filter : whereClause.filters()) {
         String column = this.mapVertexProperty(neighborVertexMapping, filter.property());
         if ("IS NULL".equals(filter.operator())) {
            existsSql.append(" AND _wv.").append(column).append(" IS NULL");
         } else {
            existsSql.append(" AND _wv.").append(column).append(" ").append(filter.operator()).append(" ?");
            params.add(filter.value());
         }
      }

      existsSql.append(")");
      return existsSql.toString();
   }

   private void appendWhereClauseForVertex(StringBuilder sql, List<Object> params, List<HasFilter> filters, VertexMapping mapping) {
      this.appendWhereClauseForVertexAlias(sql, params, filters, mapping, null);
   }

   private void appendWhereClauseForVertexAlias(StringBuilder sql, List<Object> params, List<HasFilter> filters, VertexMapping mapping, String alias) {
      if (!filters.isEmpty()) {
         StringJoiner whereJoiner = new StringJoiner(" AND ");

         for(HasFilter filter : filters) {
            String column = "id".equals(filter.property()) ? mapping.idColumn() : this.mapVertexProperty(mapping, filter.property());
            String qualifiedColumn = alias != null ? alias + "." + column : column;
            if ("IS NULL".equals(filter.operator())) {
               whereJoiner.add(qualifiedColumn + " IS NULL");
            } else {
               whereJoiner.add(qualifiedColumn + " " + filter.operator() + " ?");
               params.add(filter.value());
            }
         }

         sql.append(" WHERE ").append(whereJoiner);
      }
   }

   private void appendWhereClauseForEdge(StringBuilder sql, List<Object> params, List<HasFilter> filters, EdgeMapping mapping) {
      this.appendWhereClauseForEdgeAlias(sql, params, filters, mapping, null);
   }

   private void appendWhereClauseForEdgeAlias(StringBuilder sql, List<Object> params, List<HasFilter> filters, EdgeMapping mapping, String alias) {
      if (!filters.isEmpty()) {
         StringJoiner whereJoiner = new StringJoiner(" AND ");

         for(HasFilter filter : filters) {
            String column;
            if ("id".equals(filter.property())) {
               column = mapping.idColumn();
            } else if ("outV".equals(filter.property())) {
               column = mapping.outColumn();
            } else if ("inV".equals(filter.property())) {
               column = mapping.inColumn();
            } else {
               column = this.mapEdgeProperty(mapping, filter.property());
            }

            String qualifiedColumn = alias == null ? column : alias + "." + column;
            if ("IS NULL".equals(filter.operator())) {
               whereJoiner.add(qualifiedColumn + " IS NULL");
            } else {
               whereJoiner.add(qualifiedColumn + " " + filter.operator() + " ?");
               params.add(filter.value());
            }
         }

         sql.append(" WHERE ").append(whereJoiner);
      }
   }

   private static void appendLimit(StringBuilder sql, Integer limit) {
      if (limit != null) {
         sql.append(" LIMIT ").append(limit);
      }

   }

    private void appendOrderBy(StringBuilder sql, String orderByProperty, String orderDirection) {
       if (orderByProperty != null) {
          String direction = orderDirection != null ? orderDirection : "ASC";
          // When ordering by projected aliases, don't apply dialect quoting
          // as the alias name already matches the AS clause exactly.
          sql.append(" ORDER BY ").append(orderByProperty).append(" ").append(direction);
       }

    }

   private void appendEqAliasPredicate(StringBuilder sql, WhereClause whereClause, List<AsAlias> asAliases, String currentAlias, String idCol, boolean hasExistingWhere) {
      String targetAlias = whereClause.left();
      Integer hopIndex = null;

      for(AsAlias aa : asAliases) {
         if (aa.label().equals(targetAlias)) {
            hopIndex = aa.hopIndexAfter();
            break;
         }
      }

      if (hopIndex == null) {
         throw new IllegalArgumentException("where(eq('alias')): alias '" + targetAlias + "' not defined via .as() in the traversal");
      } else {
         String targetVertexAlias = "v" + hopIndex;
         sql.append(hasExistingWhere ? " AND " : " WHERE ").append(currentAlias).append('.').append(idCol).append(" = ").append(targetVertexAlias).append('.').append(idCol);
      }
   }

   private void appendNeqAliasPredicate(StringBuilder sql, WhereClause whereClause, List<AsAlias> asAliases, String whereByProperty, List<HopStep> hops, MappingConfig mappingConfig, VertexMapping startVertexMapping, String idCol, boolean hasExistingWhere) {
      int leftHop = this.resolveAliasHopIndex(whereClause.left(), asAliases, "left");
      int rightHop = this.resolveAliasHopIndex(whereClause.right(), asAliases, "right");
      String leftAlias = "v" + leftHop;
      String rightAlias = "v" + rightHop;
      String predicate;
      if (whereByProperty == null || whereByProperty.isBlank()) {
         predicate = leftAlias + "." + idCol + " <> " + rightAlias + "." + idCol;
      } else {
         VertexMapping leftMapping = this.resolveVertexMappingAtHop(hops, mappingConfig, startVertexMapping, leftHop);
         VertexMapping rightMapping = this.resolveVertexMappingAtHop(hops, mappingConfig, startVertexMapping, rightHop);
         String leftCol = this.mapVertexProperty(leftMapping, whereByProperty);
         String rightCol = this.mapVertexProperty(rightMapping, whereByProperty);
         predicate = leftAlias + "." + leftCol + " <> " + rightAlias + "." + rightCol;
      }

      sql.append(hasExistingWhere ? " AND " : " WHERE ").append(predicate);
   }

   private int resolveAliasHopIndex(String aliasLabel, List<AsAlias> asAliases, String side) {
      for(AsAlias alias : asAliases) {
         if (alias.label().equals(aliasLabel)) {
            return alias.hopIndexAfter();
         }
      }

      throw new IllegalArgumentException("where() " + side + " alias not found: " + aliasLabel);
   }

   private VertexMapping resolveVertexMappingAtHop(List<HopStep> hops, MappingConfig mappingConfig, VertexMapping startVertexMapping, int hopIndex) {
      VertexMapping current = startVertexMapping;

      for(int i = 0; i < Math.min(hopIndex, hops.size()); ++i) {
         HopStep hop = hops.get(i);
         if (!"outV".equals(hop.direction()) && !"inV".equals(hop.direction()) && !"outE".equals(hop.direction()) && !"inE".equals(hop.direction())) {
            EdgeMapping edgeMapping = this.resolveEdgeMapping(hop.singleLabel(), mappingConfig);
            boolean outDirection = "out".equals(hop.direction());
            current = this.resolveHopTargetVertexMapping(mappingConfig, edgeMapping, outDirection, current);
         }
      }

      return current;
   }

   private String mapVertexProperty(VertexMapping mapping, String property) {
      String column = mapping.properties().get(property);
      if (column == null) {
         throw new IllegalArgumentException("No vertex property mapping found for property: " + property);
      } else {
         return column;
      }
   }

   private String mapEdgeProperty(EdgeMapping mapping, String property) {
      String column = mapping.properties().get(property);
      if (column == null) {
         throw new IllegalArgumentException("No edge property mapping found for property: " + property);
      } else {
         return column;
      }
   }

   private String resolveSingleLabel(Map<String, ?> map, String kind) {
      if (map.size() != 1) {
         throw new IllegalArgumentException("Traversal must include hasLabel() when more than one " + kind + " label is mapped");
      } else {
         return map.keySet().iterator().next();
      }
   }

   private EdgeMapping resolveEdgeMapping(String edgeLabelOrNull, MappingConfig mappingConfig) {
      String resolvedEdgeLabel = edgeLabelOrNull;
      if (edgeLabelOrNull == null || edgeLabelOrNull.isBlank()) {
         resolvedEdgeLabel = this.resolveSingleLabel(mappingConfig.edges(), "edge");
      }

      EdgeMapping resolved = mappingConfig.edges().get(resolvedEdgeLabel);
      if (resolved == null) {
         throw new IllegalArgumentException("No edge mapping found for label: " + resolvedEdgeLabel);
      }

      return resolved;
   }

   private TranslationResult buildEdgeGroupCountSql(ParsedTraversal parsed, EdgeMapping edgeMapping) {
      if (parsed.countRequested()) {
         throw new IllegalArgumentException("count() cannot be combined with groupCount()");
      } else if (parsed.valueProperty() != null) {
         throw new IllegalArgumentException("values() cannot be combined with groupCount()");
      } else if (!parsed.projections().isEmpty()) {
         throw new IllegalArgumentException("project(...) cannot be combined with groupCount()");
      } else {
         String groupProperty = parsed.groupCountProperty();
         String mappedColumn = this.mapEdgeProperty(edgeMapping, groupProperty);
         List<Object> params = new ArrayList<>();
         StringBuilder sql = (new StringBuilder("SELECT ")).append(mappedColumn).append(" AS ").append(this.quoteAlias(groupProperty)).append(", COUNT(*) AS count").append(" FROM ").append(edgeMapping.table());
         this.appendWhereClauseForEdge(sql, params, parsed.filters(), edgeMapping);
         sql.append(" GROUP BY ").append(mappedColumn);
         if (parsed.orderByCountDesc()) {
            sql.append(" ORDER BY count ").append(parsed.orderDirection() != null ? parsed.orderDirection() : "DESC");
         } else {
            appendOrderBy(sql, parsed.orderByProperty(), parsed.orderDirection());
         }

         appendLimit(sql, parsed.limit() != null ? parsed.limit() : parsed.preHopLimit());
         return new TranslationResult(sql.toString(), params);
      }
   }

   private TranslationResult buildVertexGroupCountSql(ParsedTraversal parsed, VertexMapping vertexMapping) {
      String groupProperty = parsed.groupCountProperty();
      String mappedColumn = this.mapVertexProperty(vertexMapping, groupProperty);
      List<Object> params = new ArrayList<>();
      StringBuilder sql = (new StringBuilder("SELECT ")).append(mappedColumn).append(" AS ").append(this.quoteAlias(groupProperty)).append(", COUNT(*) AS count").append(" FROM ").append(vertexMapping.table());
      this.appendWhereClauseForVertex(sql, params, parsed.filters(), vertexMapping);
      sql.append(" GROUP BY ").append(mappedColumn);
      if (parsed.orderByCountDesc()) {
         sql.append(" ORDER BY count ").append(parsed.orderDirection() != null ? parsed.orderDirection() : "DESC");
      } else {
         appendOrderBy(sql, parsed.orderByProperty(), parsed.orderDirection());
      }

      appendLimit(sql, parsed.limit() != null ? parsed.limit() : parsed.preHopLimit());
      return new TranslationResult(sql.toString(), params);
   }

   private TranslationResult buildAliasKeyedGroupCountSql(ParsedTraversal parsed, MappingConfig mappingConfig, VertexMapping startVertexMapping) {
      GroupCountKeySpec keySpec = parsed.groupCountKeySpec();
      if (keySpec == null) {
         throw new IllegalArgumentException("groupCountKeySpec is required for alias-keyed groupCount");
      } else {
         int aliasHopIndex = -1;

         for(AsAlias aa : parsed.asAliases()) {
            if (aa.label().equals(keySpec.alias())) {
               aliasHopIndex = aa.hopIndexAfter();
               break;
            }
         }

         if (aliasHopIndex < 0) {
            throw new IllegalArgumentException("Alias '" + keySpec.alias() + "' not found in traversal; use .as('alias') before groupCount()");
         } else {
            VertexMapping keyVertexMapping = startVertexMapping;
            if (aliasHopIndex != 0) {
               for(int i = 0; i < Math.min(aliasHopIndex, parsed.hops().size()); ++i) {
                  HopStep hop = parsed.hops().get(i);
                  if (!hop.labels().isEmpty()) {
                     EdgeMapping em = this.resolveEdgeMapping(hop.labels().get(0), mappingConfig);
                     boolean outDirection = "out".equals(hop.direction()) || "outE".equals(hop.direction());
                     keyVertexMapping = this.resolveHopTargetVertexMapping(mappingConfig, em, outDirection, keyVertexMapping);
                  }
               }
            }

            String groupPropCol = this.mapVertexProperty(keyVertexMapping, keySpec.property());
            String vertexAlias = "v" + aliasHopIndex;
            String idCol = startVertexMapping.idColumn();
            List<Object> params = new ArrayList<>();
            StringBuilder sql = (new StringBuilder("SELECT ")).append(vertexAlias).append(".").append(groupPropCol).append(" AS ").append(this.quoteAlias(keySpec.property())).append(", COUNT(*) AS count").append(" FROM ").append(startVertexMapping.table()).append(" v0");

            for(int i = 0; i < parsed.hops().size(); ++i) {
               HopStep hop = parsed.hops().get(i);
               EdgeMapping edgeMapping = this.resolveEdgeMapping(hop.singleLabel(), mappingConfig);
               String edgeAlias = "e" + (i + 1);
               String fromAlias = "v" + i;
               String toAlias = "v" + (i + 1);
               String fromId = fromAlias + "." + idCol;
               String toId = toAlias + "." + idCol;
               sql.append(" JOIN ").append(edgeMapping.table()).append(" ").append(edgeAlias);
               boolean outDirection = "out".equals(hop.direction()) || "outE".equals(hop.direction());
               if (outDirection) {
                  sql.append(" ON ").append(edgeAlias).append(".").append(edgeMapping.outColumn()).append(" = ").append(fromId);
               } else {
                  sql.append(" ON ").append(edgeAlias).append(".").append(edgeMapping.inColumn()).append(" = ").append(fromId);
               }

               VertexMapping toVertexMapping = this.resolveHopTargetVertexMapping(mappingConfig, edgeMapping, outDirection, startVertexMapping);
               sql.append(" JOIN ").append(toVertexMapping.table()).append(" ").append(toAlias);
               if (outDirection) {
                  sql.append(" ON ").append(toId).append(" = ").append(edgeAlias).append(".").append(edgeMapping.inColumn());
               } else {
                  sql.append(" ON ").append(toId).append(" = ").append(edgeAlias).append(".").append(edgeMapping.outColumn());
               }
            }

            this.appendWhereClauseForVertexAlias(sql, params, parsed.filters(), startVertexMapping, "v0");
            sql.append(" GROUP BY ").append(vertexAlias).append(".").append(groupPropCol);
            if (parsed.orderByCountDesc()) {
               sql.append(" ORDER BY count ").append(parsed.orderDirection() != null ? parsed.orderDirection() : "DESC");
            } else {
               appendOrderBy(sql, parsed.orderByProperty(), parsed.orderDirection());
            }

            appendLimit(sql, parsed.limit() != null ? parsed.limit() : parsed.preHopLimit());
            return new TranslationResult(sql.toString(), params);
         }
      }
   }

   private ParsedTraversal withHops(ParsedTraversal parsed, List<HopStep> newHops) {
      return new ParsedTraversal(parsed.label(), parsed.filters(), parsed.valueProperty(), parsed.valueMapRequested(), parsed.limit(), parsed.preHopLimit(), newHops, parsed.countRequested(), parsed.sumRequested(), parsed.meanRequested(), parsed.projections(), parsed.groupCountProperty(), parsed.orderByProperty(), parsed.orderDirection(), parsed.asAliases(), parsed.selectFields(), parsed.whereClause(), parsed.dedupRequested(), parsed.pathSeen(), parsed.pathByProperties(), parsed.whereByProperty(), parsed.simplePathRequested(), parsed.groupCountKeySpec(), parsed.orderByCountDesc());
   }

   private boolean appendHopFiltersWithTargetFallback(StringBuilder sql, List<Object> params, List<HasFilter> filters, VertexMapping startVertexMapping, String finalVertexAlias, VertexMapping finalVertexMapping, ParsedTraversal parsed, MappingConfig mappingConfig) {
      boolean where = false;
      HopStep lastHop = parsed.hops().isEmpty() ? null : parsed.hops().get(parsed.hops().size() - 1);
      EdgeMapping terminalEdgeMapping = null;
      String terminalEdgeAlias = null;
      if (lastHop != null && ("outE".equals(lastHop.direction()) || "inE".equals(lastHop.direction()) || "bothE".equals(lastHop.direction()))) {
         terminalEdgeMapping = this.resolveEdgeMapping(lastHop.singleLabel(), mappingConfig);
         terminalEdgeAlias = "e" + parsed.hops().size();
      }

      for (HasFilter filter : filters) {
         String alias = "v0";
         String mappedColumn;
         if ("id".equals(filter.property())) {
            mappedColumn = startVertexMapping.idColumn();
         } else {
            try {
               mappedColumn = this.mapVertexProperty(startVertexMapping, filter.property());
            } catch (IllegalArgumentException ex1) {
               try {
                  mappedColumn = this.mapVertexProperty(finalVertexMapping, filter.property());
                  alias = finalVertexAlias;
               } catch (IllegalArgumentException ex2) {
                  if (terminalEdgeMapping == null) throw ex2;
                  mappedColumn = this.mapEdgeProperty(terminalEdgeMapping, filter.property());
                  alias = terminalEdgeAlias;
               }
            }
         }

         sql.append(where ? " AND " : " WHERE ").append(alias).append('.').append(mappedColumn);
         if ("IS NULL".equals(filter.operator())) {
            sql.append(" IS NULL");
         } else {
            sql.append(" ").append(filter.operator()).append(" ?");
            params.add(filter.value());
         }
         where = true;
      }

      return where;
   }

   private VertexMapping resolveFinalHopVertexMapping(List<HopStep> hops, MappingConfig mappingConfig, VertexMapping startVertexMapping) {
      VertexMapping current = startVertexMapping;
      for (HopStep hop : hops) {
         if (!"outV".equals(hop.direction()) && !"inV".equals(hop.direction())
               && !"outE".equals(hop.direction()) && !"inE".equals(hop.direction()) && !"bothE".equals(hop.direction())) {
            EdgeMapping edgeMapping = this.resolveEdgeMapping(hop.singleLabel(), mappingConfig);
            boolean outDirection = "out".equals(hop.direction());
            current = this.resolveHopTargetVertexMapping(mappingConfig, edgeMapping, outDirection, current);
         }
      }
      return current;
   }

   private String buildHopSumExpression(ParsedTraversal parsed, MappingConfig mappingConfig, VertexMapping startVertexMapping, String finalVertexAlias) {
      if (parsed.valueProperty() == null) {
         throw new IllegalArgumentException("sum() requires values('property') before aggregation");
      }
      if (!parsed.selectFields().isEmpty()) {
         String alias = parsed.selectFields().get(0).alias();
         Integer hopIndex = null;
         for (AsAlias asAlias : parsed.asAliases()) {
            if (asAlias.label().equals(alias)) { hopIndex = asAlias.hopIndexAfter(); break; }
         }
         if (hopIndex != null && hopIndex > 0 && hopIndex <= parsed.hops().size()) {
            HopStep hop = parsed.hops().get(hopIndex - 1);
            if ("outE".equals(hop.direction()) || "inE".equals(hop.direction()) || "bothE".equals(hop.direction())
                  || "out".equals(hop.direction()) || "in".equals(hop.direction())) {
               EdgeMapping em = this.resolveEdgeMapping(hop.singleLabel(), mappingConfig);
               return "SUM(e" + hopIndex + "." + this.mapEdgeProperty(em, parsed.valueProperty()) + ")";
            }
         }
      }
      try {
         return "SUM(" + finalVertexAlias + "." + this.mapVertexProperty(startVertexMapping, parsed.valueProperty()) + ")";
      } catch (IllegalArgumentException ex) {
         if (!parsed.hops().isEmpty()) {
            VertexMapping finalVm = this.resolveFinalHopVertexMapping(parsed.hops(), mappingConfig, startVertexMapping);
            return "SUM(" + finalVertexAlias + "." + this.mapVertexProperty(finalVm, parsed.valueProperty()) + ")";
         }
         throw ex;
      }
   }

   private String buildHopMeanExpression(ParsedTraversal parsed, MappingConfig mappingConfig, VertexMapping startVertexMapping, String finalVertexAlias) {
      if (parsed.valueProperty() == null) {
         throw new IllegalArgumentException("mean() requires values('property') before aggregation");
      }
      try {
         return "AVG(" + finalVertexAlias + "." + this.mapVertexProperty(startVertexMapping, parsed.valueProperty()) + ")";
      } catch (IllegalArgumentException ex) {
         if (!parsed.hops().isEmpty()) {
            VertexMapping finalVm = this.resolveFinalHopVertexMapping(parsed.hops(), mappingConfig, startVertexMapping);
            return "AVG(" + finalVertexAlias + "." + this.mapVertexProperty(finalVm, parsed.valueProperty()) + ")";
         }
         throw ex;
      }
   }

   private VertexMapping resolveVertexMappingForEdgeProjection(MappingConfig mappingConfig) {
      return mappingConfig.vertices().size() == 1 ? mappingConfig.vertices().values().iterator().next() : null;
   }

   private VertexMapping resolveVertexMappingByProperties(MappingConfig mappingConfig, List<String> propertyNames) {
      if (propertyNames.isEmpty()) return null;
      for (VertexMapping vm : mappingConfig.vertices().values()) {
         if (propertyNames.stream().allMatch(p -> vm.properties().containsKey(p))) return vm;
      }
      return null;
   }

   private VertexMapping resolveUniqueVertexMappingByProperty(MappingConfig mappingConfig, String propertyName) {
      List<VertexMapping> matches = new ArrayList<>();
      for (VertexMapping vm : mappingConfig.vertices().values()) {
         if (vm.properties().containsKey(propertyName)) matches.add(vm);
      }
      return matches.size() == 1 ? matches.get(0) : null;
   }

   private VertexMapping resolveTargetVertexMapping(MappingConfig mappingConfig, EdgeMapping edgeMapping, boolean outDirection) {
      if (mappingConfig.vertices().size() == 1) return mappingConfig.vertices().values().iterator().next();

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
         String unqualified = rawTable.contains(".") ? rawTable.substring(rawTable.lastIndexOf('.') + 1) : rawTable.replaceAll("^[a-z]+_", "");
         if (unqualified.equals(stem) || unqualified.startsWith(stem)) return entry.getValue();
      }
      for (Map.Entry<String, VertexMapping> entry : mappingConfig.vertices().entrySet()) {
         if (entry.getKey().equalsIgnoreCase(stem)) return entry.getValue();
      }
      for (VertexMapping vm : mappingConfig.vertices().values()) {
         if (targetCol.equals(vm.idColumn())) return vm;
      }

      String edgeTable = edgeMapping.table();
      String edgeTableBase = edgeTable.contains(".") ? edgeTable.substring(edgeTable.lastIndexOf('.') + 1) : edgeTable;
      String[] edgeParts = edgeTableBase.split("_");
      String edgeStem = outDirection ? edgeParts[edgeParts.length - 1] : edgeParts[0];
      for (Map.Entry<String, VertexMapping> entry : mappingConfig.vertices().entrySet()) {
         if (entry.getKey().equalsIgnoreCase(edgeStem)) return entry.getValue();
      }
      for (Map.Entry<String, VertexMapping> entry : mappingConfig.vertices().entrySet()) {
         String rawTable = entry.getValue().table();
         String unqualified = rawTable.contains(".") ? rawTable.substring(rawTable.lastIndexOf('.') + 1) : rawTable.replaceAll("^[a-z]+_", "");
         if (unqualified.startsWith(edgeStem)) return entry.getValue();
      }
      return null;
   }

   private String quoteAlias(String alias) {
      return this.dialect.quoteIdentifier(alias);
   }

   private List<HasFilter> parseHasChain(String hasChain) {
      List<HasFilter> filters = new ArrayList<>();
      if (hasChain == null || hasChain.isBlank()) return filters;
      Matcher m = HAS_CHAIN_ITEM.matcher(hasChain);
      while (m.find()) filters.add(new HasFilter(m.group(1), m.group(2)));
      return filters;
   }

   private RepeatHopResult parseRepeatHop(String repeatBodyRaw) {
      String text = repeatBodyRaw == null ? "" : repeatBodyRaw.trim();
      boolean hasSimplePath = text.endsWith(".simplePath()");
      if (hasSimplePath) text = text.substring(0, text.length() - ".simplePath()".length()).trim();
      if (!text.endsWith(")")) throw new IllegalArgumentException("Only repeat(out(...)), repeat(in(...)), or repeat(both(...)) is supported");

      String direction;
      int openParenIndex;
      if (text.startsWith("out(")) { direction = "out"; openParenIndex = 3; }
      else if (text.startsWith("in(")) { direction = "in"; openParenIndex = 2; }
      else if (text.startsWith("both(")) { direction = "both"; openParenIndex = 4; }
      else throw new IllegalArgumentException("Only repeat(out(...)), repeat(in(...)), or repeat(both(...)) is supported");

      String inner = text.substring(openParenIndex + 1, text.length() - 1).trim();
      if (inner.isEmpty()) return new RepeatHopResult(new HopStep(direction, List.of()), hasSimplePath);
      List<String> labels = this.splitArgs(inner).stream().map(this::unquote).toList();
      return new RepeatHopResult(new HopStep(direction, labels), hasSimplePath);
   }

   private boolean normalizeEndpointHop(String endpointStep, List<HopStep> hops) {
      if (hops.isEmpty()) return true;
      if (hops.size() > 1) {
         HopStep beforePrevious = hops.get(hops.size() - 2);
         if ("outV".equals(beforePrevious.direction()) || "inV".equals(beforePrevious.direction())) return true;
      }
      HopStep previous = hops.get(hops.size() - 1);
      if (!"outE".equals(previous.direction()) && !"inE".equals(previous.direction())) return true;
      hops.remove(hops.size() - 1);
      String normalized;
      if ("outE".equals(previous.direction())) { normalized = "inV".equals(endpointStep) ? "out" : "in"; }
      else { normalized = "inV".equals(endpointStep) ? "in" : "out"; }
      hops.add(new HopStep(normalized, previous.labels()));
      return false;
   }

   private WhereClause parseWhere(String rawWhere) {
      String expression = rawWhere == null ? "" : rawWhere.trim();
      if (expression.startsWith("and(") && expression.endsWith(")")) {
         String inner = expression.substring(4, expression.length() - 1).trim();
         List<String> args = this.splitTopLevelPredicateArgs(inner);
         if (args.size() < 2) {
            throw new IllegalArgumentException("where(and(...)) requires at least two predicates");
         }
         List<WhereClause> clauses = args.stream().map(this::parseWhere).toList();
         return new WhereClause(WhereKind.AND, null, null, List.of(), clauses);
      }
      if (expression.startsWith("or(") && expression.endsWith(")")) {
         String inner = expression.substring(3, expression.length() - 1).trim();
         List<String> args = this.splitTopLevelPredicateArgs(inner);
         if (args.size() < 2) {
            throw new IllegalArgumentException("where(or(...)) requires at least two predicates");
         }
         List<WhereClause> clauses = args.stream().map(this::parseWhere).toList();
         return new WhereClause(WhereKind.OR, null, null, List.of(), clauses);
      }
      if (expression.startsWith("not(") && expression.endsWith(")")) {
         String inner = expression.substring(4, expression.length() - 1).trim();
         if (inner.isBlank()) {
            throw new IllegalArgumentException("where(not(...)) requires one predicate");
         }
         return new WhereClause(WhereKind.NOT, null, null, List.of(), List.of(this.parseWhere(inner)));
      }

      Matcher neqMatcher = WHERE_NEQ_PATTERN.matcher(expression);
      if (neqMatcher.matches()) return new WhereClause(WhereKind.NEQ_ALIAS, neqMatcher.group(1), neqMatcher.group(2), List.of());
      Matcher eqAliasMatcher = WHERE_EQ_ALIAS_PATTERN.matcher(expression);
      if (eqAliasMatcher.matches()) return new WhereClause(WhereKind.EQ_ALIAS, eqAliasMatcher.group(1), null, List.of());
      Matcher gtMatcher = WHERE_SELECT_IS_GT_PATTERN.matcher(expression);
      if (gtMatcher.matches()) {
         boolean isGte = expression.contains(".is(gte(");
         return new WhereClause(isGte ? WhereKind.PROJECT_GTE : WhereKind.PROJECT_GT, gtMatcher.group(1), gtMatcher.group(2).trim(), List.of());
      }
      Matcher edgeExistsMatcher = WHERE_EDGE_EXISTS_PATTERN.matcher(expression);
      if (edgeExistsMatcher.matches()) return new WhereClause(WhereKind.EDGE_EXISTS, edgeExistsMatcher.group(1), edgeExistsMatcher.group(2), this.parseHasChain(edgeExistsMatcher.group(3)));
      Matcher neighborMatcher = WHERE_OUT_NEIGHBOR_HAS_PATTERN.matcher(expression);
      if (neighborMatcher.matches()) {
         WhereKind kind = "out".equals(neighborMatcher.group(1)) ? WhereKind.OUT_NEIGHBOR_HAS : WhereKind.IN_NEIGHBOR_HAS;
         return new WhereClause(kind, neighborMatcher.group(1), neighborMatcher.group(2), this.parseHasChain(neighborMatcher.group(3)));
      }
      Matcher edgeCountIsMatcher = WHERE_EDGE_COUNT_IS_PATTERN.matcher(expression);
      if (edgeCountIsMatcher.matches()) {
         String direction = edgeCountIsMatcher.group(1);
         String edgeLabel = edgeCountIsMatcher.group(2);
         String opGroup = edgeCountIsMatcher.group(4);
         String opValueGroup = edgeCountIsMatcher.group(5);
         String bareNumericGroup = edgeCountIsMatcher.group(6);
         String operator = opGroup != null ? opGroup : "eq";
         String rightValue = opValueGroup != null ? opValueGroup.trim() : bareNumericGroup;
         return new WhereClause(WhereKind.EDGE_EXISTS, direction, edgeLabel, List.of(new HasFilter("count", rightValue, operator)));
      }
      throw new IllegalArgumentException("Unsupported where() predicate: " + expression);
   }

   private List<String> splitTopLevelPredicateArgs(String text) {
      List<String> args = new ArrayList<>();
      StringBuilder current = new StringBuilder();
      boolean inSingleQuote = false;
      boolean inDoubleQuote = false;
      int depth = 0;

      for (int i = 0; i < text.length(); i++) {
         char c = text.charAt(i);
         if (c == '\'' && !inDoubleQuote) {
            inSingleQuote = !inSingleQuote;
            current.append(c);
            continue;
         }
         if (c == '"' && !inSingleQuote) {
            inDoubleQuote = !inDoubleQuote;
            current.append(c);
            continue;
         }
         if (!inSingleQuote && !inDoubleQuote) {
            if (c == '(') {
               depth++;
            } else if (c == ')') {
               depth--;
            } else if (c == ',' && depth == 0) {
               args.add(current.toString().trim());
               current.setLength(0);
               continue;
            }
         }
         current.append(c);
      }
      if (!current.isEmpty()) {
         args.add(current.toString().trim());
      }
      return args;
   }

   private static void ensureArgCount(String step, List<String> args, int expected) {
      if (args.size() != expected) throw new IllegalArgumentException(step + " expects " + expected + " argument(s)");
   }

   private HasFilter parseHasArgument(String property, String rawValue) {
      String trimmed = rawValue.trim();
      Matcher m = HAS_PREDICATE_PATTERN.matcher(trimmed);
      if (m.matches()) {
         String predicate = m.group(1);
         String innerVal = this.unquote(m.group(2).trim());
         String operator = switch (predicate) {
            case "gt" -> ">";
            case "gte" -> ">=";
            case "lt" -> "<";
            case "lte" -> "<=";
            case "neq" -> "!=";
            default -> "=";
         };
         return new HasFilter(property, innerVal, operator);
      }
      return new HasFilter(property, this.unquote(trimmed));
   }

   private GroupCountKeySpec tryParseGroupCountSelectKey(String rawExpr) {
      if (rawExpr == null || rawExpr.isBlank()) return null;
      Matcher m = GROUP_COUNT_SELECT_BY_PATTERN.matcher(rawExpr.trim());
      return m.matches() ? new GroupCountKeySpec(m.group(1), m.group(2)) : null;
   }

   private List<String> splitArgs(String argsText) {
      List<String> args = new ArrayList<>();
      StringBuilder current = new StringBuilder();
      boolean inSingleQuote = false;
      boolean inDoubleQuote = false;
      for (int i = 0; i < argsText.length(); i++) {
         char c = argsText.charAt(i);
         if (c == '\'' && !inDoubleQuote) { inSingleQuote = !inSingleQuote; current.append(c); }
         else if (c == '"' && !inSingleQuote) { inDoubleQuote = !inDoubleQuote; current.append(c); }
         else if (c == ',' && !inSingleQuote && !inDoubleQuote) { args.add(current.toString().trim()); current.setLength(0); }
         else { current.append(c); }
      }
      if (!argsText.isBlank()) args.add(current.toString().trim());
      return args;
   }

   private String unquote(String text) {
      String trimmed = text.trim();
      if ((trimmed.startsWith("\"") && trimmed.endsWith("\"")) || (trimmed.startsWith("'") && trimmed.endsWith("'"))) {
         return trimmed.substring(1, trimmed.length() - 1);
      }
      return trimmed;
   }

   private ProjectionField parseProjection(String alias, String byExpression) {
      String expression = byExpression == null ? "" : byExpression.trim();
      Matcher degreeMatcher = EDGE_DEGREE_PATTERN.matcher(expression);
      if (degreeMatcher.matches()) {
         String direction = degreeMatcher.group(1).equals("outE") ? "out" : "in";
         String edgeLabel = degreeMatcher.group(2);
         List<HasFilter> degreeFilters = this.parseHasChain(degreeMatcher.group(3));
         StringBuilder encoded = new StringBuilder(direction + ":" + edgeLabel);
         for (HasFilter f : degreeFilters) encoded.append(":").append(f.property()).append("=").append(f.value());
         return new ProjectionField(alias, ProjectionKind.EDGE_DEGREE, encoded.toString());
      }
      Matcher vertexCountMatcher = VERTEX_COUNT_PATTERN.matcher(expression);
      if (vertexCountMatcher.matches()) {
         String direction = vertexCountMatcher.group(1);
         String edgeLabel = vertexCountMatcher.group(2);
         List<HasFilter> vcFilters = this.parseHasChain(vertexCountMatcher.group(3));
         StringBuilder encoded = new StringBuilder(direction + ":" + edgeLabel);
         for (HasFilter f : vcFilters) encoded.append(":").append(f.property()).append("=").append(f.value());
         ProjectionKind kind = "out".equals(direction) ? ProjectionKind.OUT_VERTEX_COUNT : ProjectionKind.IN_VERTEX_COUNT;
         return new ProjectionField(alias, kind, encoded.toString());
      }
      Matcher neighborValuesMatcher = NEIGHBOR_VALUES_FOLD_PATTERN.matcher(expression);
      if (neighborValuesMatcher.matches()) {
         String direction = neighborValuesMatcher.group(1);
         String edgeLabel = this.splitArgs(neighborValuesMatcher.group(2)).stream().map(this::unquote).toList().get(0);
         String prop = neighborValuesMatcher.group(3);
         ProjectionKind kind = "out".equals(direction) ? ProjectionKind.OUT_NEIGHBOR_PROPERTY : ProjectionKind.IN_NEIGHBOR_PROPERTY;
         return new ProjectionField(alias, kind, direction + ":" + edgeLabel + ":" + prop);
      }
      ChooseProjectionSpec chooseSpec = this.parseChooseProjection(expression);
      if (chooseSpec != null) return new ProjectionField(alias, ProjectionKind.CHOOSE_VALUES_IS_CONSTANT, expression);
      Matcher endpointMatcher = ENDPOINT_VALUES_PATTERN.matcher(expression);
      if (endpointMatcher.matches()) {
         String endpoint = endpointMatcher.group(1);
         String property = this.unquote(endpointMatcher.group(2));
         if (property.isBlank()) throw new IllegalArgumentException("by(outV().values(...)) and by(inV().values(...)) require a property name");
         ProjectionKind kind = "outV".equals(endpoint) ? ProjectionKind.OUT_VERTEX_PROPERTY : ProjectionKind.IN_VERTEX_PROPERTY;
         return new ProjectionField(alias, kind, property);
      }
      if ("identity()".equals(expression) || "__.identity()".equals(expression)) {
         return new ProjectionField(alias, ProjectionKind.IDENTITY, "id");
      }
      if (expression.contains("(")) throw new IllegalArgumentException("Unsupported by() projection expression: " + expression);
      String property = this.unquote(expression);
      if (property.isBlank()) throw new IllegalArgumentException("by(...) property must be non-empty");
      return new ProjectionField(alias, ProjectionKind.EDGE_PROPERTY, property);
   }

   private ParsedTraversal parseSteps(List<com.graphqueryengine.query.parser.model.GremlinStep> stepNodes, String rootIdFilter) {
      String label = null;
      String valuesProperty = null;
      boolean valueMapRequested = false;
      String groupCountProperty = null;
      GroupCountKeySpec groupCountKeySpec = null;
      boolean groupCountSeen = false;
      boolean orderSeen = false;
      String orderByProperty = null;
      String orderDirection = null;
      boolean orderByCountDesc = false;
      Integer limit = null;
      Integer preHopLimit = null;
      boolean hopsSeen = false;
      boolean countRequested = false;
      boolean sumRequested = false;
      boolean meanRequested = false;
      List<HasFilter> filters = new ArrayList<>();
      List<HopStep> hops = new ArrayList<>();
      List<String> projectAliases = new ArrayList<>();
      List<String> byExpressions = new ArrayList<>();
      RepeatHopResult pendingRepeatHop = null;
      List<AsAlias> asAliases = new ArrayList<>();
      List<String> selectAliases = new ArrayList<>();
      List<String> selectByExpressions = new ArrayList<>();
      WhereClause whereClause = null;
      boolean selectSeen = false;
      boolean dedupRequested = false;
      boolean pathSeen = false;
      List<String> pathByProperties = new ArrayList<>();
      boolean simplePathRequested = false;
      String whereByProperty = null;
      boolean whereByPending = false;

      if (rootIdFilter != null) filters.add(new HasFilter("id", rootIdFilter));

      for (com.graphqueryengine.query.parser.model.GremlinStep stepNode : stepNodes) {
         String stepName = stepNode.name();
         List<String> args = stepNode.args();
         String rawStepArgs = stepNode.rawArgs() == null ? "" : stepNode.rawArgs().trim();
         if (whereByPending && !"by".equals(stepName)) whereByPending = false;

         switch (stepName) {
            case "hasLabel" -> { ensureArgCount(stepName, args, 1); label = this.unquote(args.get(0)); }
            case "has" -> {
               ensureArgCount(stepName, args, 2);
               filters.add(this.parseHasArgument(this.unquote(args.get(0)), args.get(1).trim()));
            }
            case "hasId" -> {
               ensureArgCount(stepName, args, 1);
               filters.add(new HasFilter("id", this.unquote(args.get(0))));
            }
            case "hasNot" -> {
               ensureArgCount(stepName, args, 1);
               filters.add(new HasFilter(this.unquote(args.get(0)), null, "IS NULL"));
            }
            case "values" -> { ensureArgCount(stepName, args, 1); valuesProperty = this.unquote(args.get(0)); }
            case "is" -> {
               ensureArgCount(stepName, args, 1);
               if (valuesProperty == null || valuesProperty.isBlank()) {
                  throw new IllegalArgumentException("is(...) is only supported after values('property')");
               }
               filters.add(this.parseHasArgument(valuesProperty, args.get(0).trim()));
            }
            case "valueMap" -> {
               if (!args.isEmpty()) throw new IllegalArgumentException("valueMap() currently supports zero arguments only");
               valueMapRequested = true;
            }
            case "out" -> {
               if (args.isEmpty()) throw new IllegalArgumentException("out expects at least 1 argument");
               hopsSeen = true;
               hops.add(new HopStep("out", args.stream().map(this::unquote).toList()));
            }
             case "in" -> {
                if (args.isEmpty()) throw new IllegalArgumentException("in expects at least 1 argument");
                hopsSeen = true;
                hops.add(new HopStep("in", args.stream().map(this::unquote).toList()));
             }
             case "both" -> {
                if (args.isEmpty()) throw new IllegalArgumentException("both expects at least 1 argument");
                hopsSeen = true;
                hops.add(new HopStep("both", args.stream().map(this::unquote).toList()));
             }
             case "outV" -> {
               ensureArgCount(stepName, args, 0);
               hopsSeen = true;
               if (this.normalizeEndpointHop("outV", hops)) hops.add(new HopStep("outV", List.of()));
            }
            case "inV" -> {
               ensureArgCount(stepName, args, 0);
               hopsSeen = true;
               if (this.normalizeEndpointHop("inV", hops)) hops.add(new HopStep("inV", List.of()));
            }
            case "bothV" -> {
               ensureArgCount(stepName, args, 0);
               hopsSeen = true;
               hops.add(new HopStep("bothV", List.of()));
            }
            case "otherV" -> {
               ensureArgCount(stepName, args, 0);
               hopsSeen = true;
               if (hops.isEmpty()) {
                  throw new IllegalArgumentException("otherV() must follow outE() or inE()");
               }

               HopStep previous = hops.get(hops.size() - 1);
               if ("outE".equals(previous.direction())) {
                  if (this.normalizeEndpointHop("inV", hops)) hops.add(new HopStep("inV", List.of()));
               } else if ("inE".equals(previous.direction())) {
                  if (this.normalizeEndpointHop("outV", hops)) hops.add(new HopStep("outV", List.of()));
               } else {
                  throw new IllegalArgumentException("otherV() must follow outE() or inE()");
               }
            }
            case "repeat" -> {
               if (rawStepArgs.isBlank()) throw new IllegalArgumentException("repeat() requires an argument");
               hopsSeen = true;
               pendingRepeatHop = this.parseRepeatHop(rawStepArgs);
            }
            case "simplePath" -> { ensureArgCount(stepName, args, 0); simplePathRequested = true; }
            case "times" -> {
               if (pendingRepeatHop == null) throw new IllegalArgumentException("times() must follow repeat(out(...)), repeat(in(...)), or repeat(both(...))");
               ensureArgCount(stepName, args, 1);
               int repeatCount;
               try { repeatCount = Integer.parseInt(args.get(0).trim()); }
               catch (NumberFormatException ex) { throw new IllegalArgumentException("times() must be numeric"); }
               if (repeatCount < 0) throw new IllegalArgumentException("times() must be >= 0");
               if (pendingRepeatHop.simplePathDetected()) simplePathRequested = true;
               HopStep repeatHop = pendingRepeatHop.hop();
               if (repeatHop.labels().size() == repeatCount && repeatCount > 1) {
                  for (String lbl : repeatHop.labels()) hops.add(new HopStep(repeatHop.direction(), lbl));
               } else {
                  for (int i = 0; i < repeatCount; i++) hops.add(repeatHop);
               }
               pendingRepeatHop = null;
            }
            case "limit" -> {
               if (args.isEmpty() || args.size() > 2) throw new IllegalArgumentException("limit expects 1 or 2 argument(s)");
               String limitArg = args.get(args.size() - 1).trim();
               int parsedLimit;
               try { parsedLimit = Integer.parseInt(limitArg); }
               catch (NumberFormatException ex) { throw new IllegalArgumentException("limit() must be numeric"); }
               if (!hopsSeen) preHopLimit = parsedLimit; else limit = parsedLimit;
            }
            case "count" -> { ensureArgCount(stepName, args, 0); countRequested = true; }
            case "sum" -> { ensureArgCount(stepName, args, 0); sumRequested = true; }
            case "mean" -> { ensureArgCount(stepName, args, 0); meanRequested = true; }
            case "groupCount" -> {
               ensureArgCount(stepName, args, 0);
               if (groupCountSeen) throw new IllegalArgumentException("Only a single groupCount() step is supported");
               groupCountSeen = true;
            }
            case "project" -> {
               if (args.isEmpty()) throw new IllegalArgumentException("project expects at least 1 argument");
               if (!projectAliases.isEmpty()) throw new IllegalArgumentException("Only a single project(...) step is supported");
               for (String arg : args) {
                  String a = this.unquote(arg);
                  if (a.isBlank()) throw new IllegalArgumentException("project aliases must be non-empty");
                  projectAliases.add(a);
               }
            }
            case "outE" -> {
               if (args.size() > 1) throw new IllegalArgumentException("outE expects 0 or 1 argument(s)");
               hopsSeen = true;
               hops.add(new HopStep("outE", args.isEmpty() ? List.of() : List.of(this.unquote(args.get(0)))));
            }
            case "inE" -> {
               if (args.size() > 1) throw new IllegalArgumentException("inE expects 0 or 1 argument(s)");
               hopsSeen = true;
               hops.add(new HopStep("inE", args.isEmpty() ? List.of() : List.of(this.unquote(args.get(0)))));
            }
            case "bothE" -> {
               if (args.size() > 1) throw new IllegalArgumentException("bothE expects 0 or 1 argument(s)");
               hopsSeen = true;
               hops.add(new HopStep("bothE", args.isEmpty() ? List.of() : List.of(this.unquote(args.get(0)))));
            }
            case "as" -> {
               ensureArgCount(stepName, args, 1);
               String aliasName = this.unquote(args.get(0));
               if (aliasName.isBlank()) throw new IllegalArgumentException("as() label must be non-empty");
               asAliases.add(new AsAlias(aliasName, hops.size()));
            }
            case "select" -> {
               if (args.isEmpty()) throw new IllegalArgumentException("select expects at least 1 argument");
               if (selectSeen) throw new IllegalArgumentException("Only a single select(...) step is supported");
               selectSeen = true;
               for (String arg : args) selectAliases.add(this.unquote(arg));
            }
            case "where" -> {
               if (whereClause != null) throw new IllegalArgumentException("Only a single where() step is supported");
               whereClause = this.parseWhere(rawStepArgs);
               whereByPending = whereClause.kind() == WhereKind.NEQ_ALIAS;
            }
            case "dedup" -> { ensureArgCount(stepName, args, 0); dedupRequested = true; }
            case "identity" -> ensureArgCount(stepName, args, 0);
            case "path" -> { ensureArgCount(stepName, args, 0); pathSeen = true; }
            case "by" -> {
               if (args.isEmpty()) throw new IllegalArgumentException("by(...) expects at least 1 argument");
               if (whereByPending) {
                  ensureArgCount(stepName, args, 1);
                  whereByProperty = this.unquote(args.get(0));
                  whereByPending = false;
               } else if (selectSeen) {
                  if (selectByExpressions.size() >= selectAliases.size()) throw new IllegalArgumentException("select(...) has more by(...) modulators than selected aliases");
                  selectByExpressions.add(rawStepArgs);
                } else if (orderSeen && orderByProperty == null && !orderByCountDesc) {
                   List<String> byArgsTrimmed = args.stream().map(String::trim).toList();
                   if (byArgsTrimmed.size() == 2 && ("values".equals(byArgsTrimmed.get(0)) || "__.values()".equals(byArgsTrimmed.get(0)))) {
                      String dirToken = byArgsTrimmed.get(1).toLowerCase();
                      orderByCountDesc = true;
                      orderDirection = (dirToken.equals("desc") || dirToken.equals("order.desc")) ? "DESC" : "ASC";
                   } else if (byArgsTrimmed.size() == 2 && !byArgsTrimmed.get(0).contains("(")) {
                      // Handle .by(propertyName, Order.desc) or .by(propertyName, Order.asc)
                      orderByProperty = this.unquote(byArgsTrimmed.get(0));
                      String dirToken = byArgsTrimmed.get(1).toLowerCase();
                      orderDirection = (dirToken.contains("desc") || dirToken.equals("order.desc")) ? "DESC" : "ASC";
                   } else if (rawStepArgs.contains("select(") && rawStepArgs.contains("Order.")) {
                      int selectStart = rawStepArgs.indexOf("select(") + 7;
                      int selectEnd = rawStepArgs.indexOf(")", selectStart);
                      orderByProperty = this.unquote(rawStepArgs.substring(selectStart, selectEnd));
                      orderDirection = rawStepArgs.contains("Order.desc") ? "DESC" : "ASC";
                   } else {
                      orderByProperty = this.unquote(rawStepArgs);
                      orderDirection = "ASC";
                   }
               } else if (groupCountSeen && groupCountProperty == null && groupCountKeySpec == null) {
                  GroupCountKeySpec parsedKey = this.tryParseGroupCountSelectKey(rawStepArgs);
                  if (parsedKey != null) { groupCountKeySpec = parsedKey; }
                  else {
                     ensureArgCount(stepName, args, 1);
                     groupCountProperty = this.unquote(args.get(0));
                     if (groupCountProperty.isBlank()) throw new IllegalArgumentException("by(...) property must be non-empty");
                  }
               } else if (!projectAliases.isEmpty()) {
                  if (byExpressions.size() >= projectAliases.size()) throw new IllegalArgumentException("project(...) has more by(...) modulators than projected fields");
                  byExpressions.add(rawStepArgs);
               } else if (pathSeen) {
                  ensureArgCount(stepName, args, 1);
                  pathByProperties.add(this.unquote(args.get(0)));
               } else {
                  throw new IllegalArgumentException("by(...) is only supported after project(...), groupCount(...), or order(...)");
               }
            }
            case "order" -> {
               if (args.size() > 1) throw new IllegalArgumentException("order expects 0 or 1 argument(s)");
               if (orderSeen) throw new IllegalArgumentException("Only a single order() step is supported");
               orderSeen = true;
            }
            default -> throw new IllegalArgumentException("Unsupported step: " + stepName);
         }
      }

      if (pendingRepeatHop != null) throw new IllegalArgumentException("repeat(...) must be followed by times(n)");
      if (!projectAliases.isEmpty() && byExpressions.size() != projectAliases.size())
         throw new IllegalArgumentException("project(...) requires one by(...) modulator per projected field");

      if (pathSeen && pathByProperties.size() == 1 && valuesProperty == null) valuesProperty = pathByProperties.get(0);

      List<ProjectionField> projections = new ArrayList<>();
      for (int i = 0; i < projectAliases.size(); i++) projections.add(this.parseProjection(projectAliases.get(i), byExpressions.get(i)));

      List<SelectField> selectFields = new ArrayList<>();
      String sharedSelectBy = (selectAliases.size() > 1 && selectByExpressions.size() == 1) ? this.unquote(selectByExpressions.get(0)) : null;
      for (int i = 0; i < selectAliases.size(); i++) {
         String property = sharedSelectBy != null ? sharedSelectBy : (i < selectByExpressions.size() ? this.unquote(selectByExpressions.get(i)) : null);
         selectFields.add(new SelectField(selectAliases.get(i), property));
      }

      if ((sumRequested || meanRequested) && valuesProperty == null) {
         if (selectFields.size() == 1) {
            SelectField sf = selectFields.get(0);
            if (sf.property() != null && !sf.property().isBlank()) {
               valuesProperty = sf.property();
            } else {
               for (ProjectionField pf : projections) {
                  if (pf.alias().equals(sf.alias()) && pf.kind() == ProjectionKind.EDGE_PROPERTY) { valuesProperty = pf.property(); break; }
               }
            }
         }
         if (valuesProperty == null)
            throw new IllegalArgumentException((meanRequested ? "mean()" : "sum()") + " requires values('property') before aggregation");
      }

      return new ParsedTraversal(label, filters, valuesProperty, valueMapRequested, limit, preHopLimit, hops, countRequested, sumRequested, meanRequested, projections, groupCountProperty, orderByProperty, orderDirection, asAliases, selectFields, whereClause, dedupRequested, pathSeen, pathByProperties, whereByProperty, simplePathRequested, groupCountKeySpec, orderByCountDesc);
   }

   // ── Inner types ───────────────────────────────────────────────────────────

   private record RepeatHopResult(HopStep hop, boolean simplePathDetected) {}

   private record HopStep(String direction, List<String> labels) {
      HopStep(String direction, String singleLabel) {
         this(direction, singleLabel == null ? List.of() : List.of(singleLabel));
      }
      String singleLabel() {
         if (labels.isEmpty()) return null;
         if (labels.size() > 1) throw new IllegalStateException("singleLabel() called on multi-label HopStep: " + labels);
         return labels.get(0);
      }
   }

   private record ParsedTraversal(
         String label, List<HasFilter> filters, String valueProperty, boolean valueMapRequested,
         Integer limit, Integer preHopLimit, List<HopStep> hops,
         boolean countRequested, boolean sumRequested, boolean meanRequested,
         List<ProjectionField> projections, String groupCountProperty,
         String orderByProperty, String orderDirection, List<AsAlias> asAliases,
         List<SelectField> selectFields, WhereClause whereClause,
         boolean dedupRequested, boolean pathSeen, List<String> pathByProperties,
         String whereByProperty, boolean simplePathRequested,
         GroupCountKeySpec groupCountKeySpec, boolean orderByCountDesc) {}

   private record GroupCountKeySpec(String alias, String property) {}

   private record ChooseProjectionSpec(String property, String predicate, String predicateValue, String trueConstant, String falseConstant) {}

   private enum ProjectionKind {
      EDGE_PROPERTY, OUT_VERTEX_PROPERTY, IN_VERTEX_PROPERTY,
      EDGE_DEGREE, OUT_VERTEX_COUNT, IN_VERTEX_COUNT,
      OUT_NEIGHBOR_PROPERTY, IN_NEIGHBOR_PROPERTY, CHOOSE_VALUES_IS_CONSTANT, IDENTITY
   }

   private record ProjectionField(String alias, ProjectionKind kind, String property) {}

   private record AsAlias(String label, int hopIndexAfter) {}

   private record SelectField(String alias, String property) {}

   private record WhereClause(WhereKind kind, String left, String right, List<HasFilter> filters, List<WhereClause> clauses) {
      WhereClause(WhereKind kind, String left, String right, List<HasFilter> filters) {
         this(kind, left, right, filters, List.of());
      }
   }

   private enum WhereKind {
      NEQ_ALIAS, EQ_ALIAS, PROJECT_GT, PROJECT_GTE, EDGE_EXISTS, OUT_NEIGHBOR_HAS, IN_NEIGHBOR_HAS, AND, OR, NOT
   }
}
