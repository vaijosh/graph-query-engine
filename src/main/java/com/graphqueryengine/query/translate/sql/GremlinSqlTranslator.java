package com.graphqueryengine.query.translate.sql;

import com.graphqueryengine.mapping.EdgeMapping;
import com.graphqueryengine.mapping.MappingConfig;
import com.graphqueryengine.mapping.VertexMapping;
import com.graphqueryengine.query.api.TranslationResult;
import com.graphqueryengine.query.parser.LegacyGremlinTraversalParser;
import com.graphqueryengine.query.parser.model.GremlinParseResult;
import com.graphqueryengine.query.parser.model.GremlinStep;
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
   private static final Pattern WHERE_OUT_NEIGHBOR_HAS_PATTERN = Pattern.compile("^(out|in)\\(['\"]([^'\"]+)['\"]\\)((?:\\.has\\(['\"][^'\"]+['\"],\\s*['\"][^'\"]*['\"]\\))+)$");
   private static final Pattern HAS_PREDICATE_PATTERN = Pattern.compile("^(gt|gte|lt|lte|neq|eq)\\((.+)\\)$");
   private static final Pattern CHOOSE_VALUES_IS_CONSTANT_PATTERN = Pattern.compile("^choose\\(\\s*values\\(['\"]([^'\"]+)['\"]\\)\\.is\\((gt|gte|lt|lte|eq|neq)\\(([^)]+)\\)\\)\\s*,\\s*constant\\(([^)]+)\\)\\s*,\\s*constant\\(([^)]+)\\)\\s*\\)$");
   private static final Pattern GROUP_COUNT_SELECT_BY_PATTERN = Pattern.compile("^select\\(['\"]([^'\"]+)['\"]\\)\\.by\\(['\"]([^'\"]+)['\"]\\)$");

   public GremlinSqlTranslator() {
   }

   public TranslationResult translate(String gremlin, MappingConfig mappingConfig) {
      GremlinParseResult parsed = (new LegacyGremlinTraversalParser()).parse(gremlin);
      return this.translate(parsed, mappingConfig);
   }

   public TranslationResult translate(GremlinParseResult parsed, MappingConfig mappingConfig) {
      if (parsed == null) {
         throw new IllegalArgumentException("Parsed Gremlin query is required");
      } else {
         ParsedTraversal parsedTraversal = this.parseSteps(parsed.steps(), parsed.rootIdFilter());
         return parsed.vertexQuery() ? this.buildVertexSql(parsedTraversal, mappingConfig) : this.buildEdgeSql(parsedTraversal, mappingConfig);
      }
   }

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
            if (parsed.whereClause().kind() != GremlinSqlTranslator.WhereKind.EDGE_EXISTS && parsed.whereClause().kind() != GremlinSqlTranslator.WhereKind.OUT_NEIGHBOR_HAS && parsed.whereClause().kind() != GremlinSqlTranslator.WhereKind.IN_NEIGHBOR_HAS) {
               throw new IllegalArgumentException("Unsupported where() predicate for g.V(): " + parsed.whereClause().kind());
            }

            if (parsed.whereClause().kind() == GremlinSqlTranslator.WhereKind.EDGE_EXISTS) {
               this.appendEdgeExistsPredicate(sql, params, parsed.whereClause(), mappingConfig, vertexMapping, null, !parsed.filters().isEmpty());
            } else {
               this.appendNeighborHasPredicate(sql, params, parsed.whereClause(), mappingConfig, vertexMapping, null, !parsed.filters().isEmpty());
            }
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
            String finalVertexAlias = "v" + hopCount;
            String idCol = startVertexMapping.idColumn();
            if (parsed.countRequested()) {
               if (parsed.dedupRequested()) {
                  sql.append("SELECT COUNT(DISTINCT ").append(finalVertexAlias).append('.').append(idCol).append(") AS count");
               } else {
                  sql.append("SELECT COUNT(*) AS count");
               }
            } else if (parsed.sumRequested()) {
               sql.append("SELECT ").append(this.buildHopSumExpression(parsed, mappingConfig, startVertexMapping, finalVertexAlias)).append(" AS sum");
            } else if (parsed.meanRequested()) {
               sql.append("SELECT ").append(this.buildHopMeanExpression(parsed, mappingConfig, startVertexMapping, finalVertexAlias)).append(" AS mean");
            } else if (!parsed.projections().isEmpty()) {
               StringJoiner projectionSelect = new StringJoiner(", ");

               for(ProjectionField projection : parsed.projections()) {
                  if (projection.kind() != GremlinSqlTranslator.ProjectionKind.EDGE_PROPERTY) {
                     throw new IllegalArgumentException("Hop projection currently supports by('property') only");
                  }

                  String mappedColumn = this.mapVertexProperty(startVertexMapping, projection.property());
                  projectionSelect.add(finalVertexAlias + "." + mappedColumn + " AS " + this.quoteAlias(projection.alias()));
               }

               sql.append(parsed.dedupRequested() ? "SELECT DISTINCT " : "SELECT ").append(projectionSelect);
            } else if (parsed.pathSeen() && !parsed.pathByProperties().isEmpty()) {
               sql.append(this.buildPathSelectClause(parsed.pathByProperties(), hopCount, mappingConfig, startVertexMapping, parsed.hops()));
            } else if (parsed.valueProperty() != null) {
               String mappedColumn = this.mapVertexProperty(startVertexMapping, parsed.valueProperty());
               sql.append("SELECT ").append(parsed.dedupRequested() ? "DISTINCT " : "").append(finalVertexAlias).append('.').append(mappedColumn).append(" AS ").append(parsed.valueProperty());
            } else {
               sql.append(parsed.dedupRequested() ? "SELECT DISTINCT " : "SELECT ").append(finalVertexAlias).append(".*");
            }

            boolean useStartSubquery = parsed.preHopLimit() != null;
            if (useStartSubquery) {
               StringBuilder startSubquery = (new StringBuilder("(SELECT * FROM ")).append(startVertexMapping.table());
               List<Object> subParams = new ArrayList<>();
               this.appendWhereClauseForVertex(startSubquery, subParams, parsed.filters(), startVertexMapping);
               if (parsed.whereClause() != null) {
                  if (parsed.whereClause().kind() == GremlinSqlTranslator.WhereKind.EDGE_EXISTS) {
                     this.appendEdgeExistsPredicate(startSubquery, subParams, parsed.whereClause(), mappingConfig, startVertexMapping, null, !parsed.filters().isEmpty());
                  } else {
                     if (parsed.whereClause().kind() != GremlinSqlTranslator.WhereKind.OUT_NEIGHBOR_HAS && parsed.whereClause().kind() != GremlinSqlTranslator.WhereKind.IN_NEIGHBOR_HAS) {
                        throw new IllegalArgumentException("Unsupported where() predicate for hop traversal: " + parsed.whereClause().kind());
                     }

                     this.appendNeighborHasPredicate(startSubquery, subParams, parsed.whereClause(), mappingConfig, startVertexMapping, null, !parsed.filters().isEmpty());
                  }
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
               if ("out".equals(hop.direction()) || "outE".equals(hop.direction())) {
                  sql.append(" ON ").append(edgeOut).append(" = ").append(fromId);
               } else {
                  sql.append(" ON ").append(edgeIn).append(" = ").append(fromId);
               }

               if (!"outE".equals(hop.direction()) && !"inE".equals(hop.direction())) {
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
                  hasWhere = this.appendHopFiltersWithTargetFallback(sql, params, parsed.filters(), startVertexMapping, finalVertexAlias, this.resolveFinalHopVertexMapping(parsed.hops(), mappingConfig, startVertexMapping), parsed, mappingConfig, hasWhere);
               }

               if (parsed.whereClause() != null) {
                  if (parsed.whereClause().kind() == GremlinSqlTranslator.WhereKind.EDGE_EXISTS) {
                     this.appendEdgeExistsPredicate(sql, params, parsed.whereClause(), mappingConfig, startVertexMapping, "v0", hasWhere);
                  } else if (parsed.whereClause().kind() != GremlinSqlTranslator.WhereKind.OUT_NEIGHBOR_HAS && parsed.whereClause().kind() != GremlinSqlTranslator.WhereKind.IN_NEIGHBOR_HAS) {
                     if (parsed.whereClause().kind() == GremlinSqlTranslator.WhereKind.EQ_ALIAS) {
                        this.appendEqAliasPredicate(sql, parsed.whereClause(), parsed.asAliases(), finalVertexAlias, idCol, hasWhere);
                     } else {
                        if (parsed.whereClause().kind() != GremlinSqlTranslator.WhereKind.NEQ_ALIAS) {
                           throw new IllegalArgumentException("Unsupported where() predicate for hop traversal: " + parsed.whereClause().kind());
                        }

                        this.appendNeqAliasPredicate(sql, parsed.whereClause(), parsed.asAliases(), parsed.whereByProperty(), parsed.hops(), mappingConfig, startVertexMapping, idCol, hasWhere);
                     }
                  } else {
                     this.appendNeighborHasPredicate(sql, params, parsed.whereClause(), mappingConfig, startVertexMapping, "v0", hasWhere);
                  }

                  hasWhere = true;
               }
            }

            if (parsed.simplePathRequested() && hopCount > 0) {
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

               sql.append(hasWhere ? " AND " : " WHERE ").append(cycleConditions);
            }

            appendLimit(sql, parsed.limit());
            return new TranslationResult(sql.toString(), params);
         }
      }
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
            StringJoiner pj = new StringJoiner(", ");

            for(ProjectionField pf : parsed.projections()) {
               if (pf.kind() != GremlinSqlTranslator.ProjectionKind.EDGE_PROPERTY) {
                  throw new IllegalArgumentException("Hop projection currently supports by('property') only");
               }

               pj.add(finalVertexAlias + "." + this.mapVertexProperty(startVertexMapping, pf.property()) + " AS " + this.quoteAlias(pf.alias()));
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
               if (parsed.whereClause().kind() == GremlinSqlTranslator.WhereKind.EDGE_EXISTS) {
                  this.appendEdgeExistsPredicate(sq, sqp, parsed.whereClause(), mappingConfig, startVertexMapping, null, !parsed.filters().isEmpty());
               } else if (parsed.whereClause().kind() == GremlinSqlTranslator.WhereKind.OUT_NEIGHBOR_HAS || parsed.whereClause().kind() == GremlinSqlTranslator.WhereKind.IN_NEIGHBOR_HAS) {
                  this.appendNeighborHasPredicate(sq, sqp, parsed.whereClause(), mappingConfig, startVertexMapping, null, !parsed.filters().isEmpty());
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
               if (parsed.whereClause().kind() == GremlinSqlTranslator.WhereKind.EDGE_EXISTS) {
                  this.appendEdgeExistsPredicate(sql, params, parsed.whereClause(), mappingConfig, startVertexMapping, "v0", hasWhere);
               } else if (parsed.whereClause().kind() == GremlinSqlTranslator.WhereKind.OUT_NEIGHBOR_HAS || parsed.whereClause().kind() == GremlinSqlTranslator.WhereKind.IN_NEIGHBOR_HAS) {
                  this.appendNeighborHasPredicate(sql, params, parsed.whereClause(), mappingConfig, startVertexMapping, "v0", hasWhere);
               }

               hasWhere = true;
            }
         }

         if (parsed.simplePathRequested() && hopCount > 0) {
            StringJoiner cc = new StringJoiner(" AND ");

            for(int i = 1; i <= hopCount; ++i) {
               String cur = "v" + i + "." + idCol;
               if (i == 1) {
                  cc.add(cur + " <> v0." + idCol);
               } else {
                  StringJoiner prior = new StringJoiner(", ");

                  for(int j = 0; j < i; ++j) {
                     prior.add("v" + j + "." + idCol);
                  }

                  cc.add(cur + " NOT IN (" + prior + ")");
               }
            }

            sql.append(hasWhere ? " AND " : " WHERE ").append(cc);
         }

         appendLimit(sql, parsed.limit());
         return new TranslationResult(sql.toString(), params);
      }
   }

   private boolean appendHopFiltersWithTargetFallback(StringBuilder sql, List<Object> params, List<HasFilter> filters, VertexMapping startVertexMapping, String finalVertexAlias, VertexMapping finalVertexMapping, ParsedTraversal parsed, MappingConfig mappingConfig, boolean hasWhere) {
      boolean where = hasWhere;
      HopStep lastHop = parsed.hops().isEmpty() ? null : parsed.hops().get(parsed.hops().size() - 1);
      EdgeMapping terminalEdgeMapping = null;
      String terminalEdgeAlias = null;
      if (lastHop != null && ("outE".equals(lastHop.direction()) || "inE".equals(lastHop.direction()))) {
         terminalEdgeMapping = this.resolveEdgeMapping(lastHop.singleLabel(), mappingConfig);
         terminalEdgeAlias = "e" + parsed.hops().size();
      }

      for(HasFilter filter : filters) {
         String alias = "v0";
         String mappedColumn;
         if ("id".equals(filter.property())) {
            mappedColumn = startVertexMapping.idColumn();
         } else {
            try {
               mappedColumn = this.mapVertexProperty(startVertexMapping, filter.property());
            } catch (IllegalArgumentException var16) {
               try {
                  mappedColumn = this.mapVertexProperty(finalVertexMapping, filter.property());
                  alias = finalVertexAlias;
               } catch (IllegalArgumentException var15) {
                  if (terminalEdgeMapping == null) {
                     throw var15;
                  }

                  mappedColumn = this.mapEdgeProperty(terminalEdgeMapping, filter.property());
                  alias = terminalEdgeAlias;
               }
            }
         }

         sql.append(where ? " AND " : " WHERE ").append(alias).append('.').append(mappedColumn).append(" ").append(filter.operator()).append(" ?");
         params.add(filter.value());
         where = true;
      }

      return where;
   }

   private VertexMapping resolveFinalHopVertexMapping(List<HopStep> hops, MappingConfig mappingConfig, VertexMapping startVertexMapping) {
      VertexMapping current = startVertexMapping;

      for(HopStep hop : hops) {
         if (!"outV".equals(hop.direction()) && !"inV".equals(hop.direction()) && !"outE".equals(hop.direction()) && !"inE".equals(hop.direction())) {
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
      } else {
         if (!parsed.selectFields().isEmpty()) {
            String alias = parsed.selectFields().get(0).alias();
            Integer hopIndex = null;

            for(AsAlias asAlias : parsed.asAliases()) {
               if (asAlias.label().equals(alias)) {
                  hopIndex = asAlias.hopIndexAfter();
                  break;
               }
            }

            if (hopIndex != null && hopIndex > 0 && hopIndex <= parsed.hops().size()) {
               HopStep hop = parsed.hops().get(hopIndex - 1);
               if ("outE".equals(hop.direction()) || "inE".equals(hop.direction()) || "out".equals(hop.direction()) || "in".equals(hop.direction())) {
                  EdgeMapping edgeMapping = this.resolveEdgeMapping(hop.singleLabel(), mappingConfig);
                  String edgeCol = this.mapEdgeProperty(edgeMapping, parsed.valueProperty());
                  return "SUM(e" + hopIndex + "." + edgeCol + ")";
               }
            }
         }

         try {
            String mappedColumn = this.mapVertexProperty(startVertexMapping, parsed.valueProperty());
            return "SUM(" + finalVertexAlias + "." + mappedColumn + ")";
         } catch (IllegalArgumentException var10) {
            if (!parsed.hops().isEmpty()) {
               HopStep lastHop = parsed.hops().get(parsed.hops().size() - 1);
               if ("outE".equals(lastHop.direction()) || "inE".equals(lastHop.direction())) {
                  EdgeMapping edgeMapping = this.resolveEdgeMapping(lastHop.singleLabel(), mappingConfig);
                  String edgeCol = this.mapEdgeProperty(edgeMapping, parsed.valueProperty());
                  return "SUM(e" + parsed.hops().size() + "." + edgeCol + ")";
               }
            }

            throw var10;
         }
      }
   }

   private String buildHopMeanExpression(ParsedTraversal parsed, MappingConfig mappingConfig, VertexMapping startVertexMapping, String finalVertexAlias) {
      if (parsed.valueProperty() == null) {
         throw new IllegalArgumentException("mean() requires values('property') before aggregation");
      } else {
         if (!parsed.selectFields().isEmpty()) {
            String alias = parsed.selectFields().get(0).alias();
            Integer hopIndex = null;

            for(AsAlias asAlias : parsed.asAliases()) {
               if (asAlias.label().equals(alias)) {
                  hopIndex = asAlias.hopIndexAfter();
                  break;
               }
            }

            if (hopIndex != null && hopIndex > 0 && hopIndex <= parsed.hops().size()) {
               HopStep hop = parsed.hops().get(hopIndex - 1);
               if ("outE".equals(hop.direction()) || "inE".equals(hop.direction()) || "out".equals(hop.direction()) || "in".equals(hop.direction())) {
                  EdgeMapping edgeMapping = this.resolveEdgeMapping(hop.singleLabel(), mappingConfig);
                  String edgeCol = this.mapEdgeProperty(edgeMapping, parsed.valueProperty());
                  return "AVG(e" + hopIndex + "." + edgeCol + ")";
               }
            }
         }

         try {
            String mappedColumn = this.mapVertexProperty(startVertexMapping, parsed.valueProperty());
            return "AVG(" + finalVertexAlias + "." + mappedColumn + ")";
         } catch (IllegalArgumentException var10) {
            if (!parsed.hops().isEmpty()) {
               HopStep lastHop = parsed.hops().get(parsed.hops().size() - 1);
               if ("outE".equals(lastHop.direction()) || "inE".equals(lastHop.direction())) {
                  EdgeMapping edgeMapping = this.resolveEdgeMapping(lastHop.singleLabel(), mappingConfig);
                  String edgeCol = this.mapEdgeProperty(edgeMapping, parsed.valueProperty());
                  return "AVG(e" + parsed.hops().size() + "." + edgeCol + ")";
               }
            }

            throw var10;
         }
      }
   }

   private ParsedTraversal withHops(ParsedTraversal parsed, List<HopStep> newHops) {
      return new ParsedTraversal(parsed.label(), parsed.filters(), parsed.valueProperty(), parsed.limit(), parsed.preHopLimit(), newHops, parsed.countRequested(), parsed.sumRequested(), parsed.meanRequested(), parsed.projections(), parsed.groupCountProperty(), parsed.orderByProperty(), parsed.orderDirection(), parsed.asAliases(), parsed.selectFields(), parsed.whereClause(), parsed.dedupRequested(), parsed.pathSeen(), parsed.pathByProperties(), parsed.whereByProperty(), parsed.simplePathRequested(), parsed.groupCountKeySpec(), parsed.orderByCountDesc());
   }

   private TranslationResult buildBothHopUnionSql(ParsedTraversal parsed, MappingConfig mappingConfig, VertexMapping startVertexMapping) {
      int hopCount = parsed.hops().size();
      String idCol = startVertexMapping.idColumn();
      String finalVertexAlias = "v" + hopCount;
      String selectClause;
      if (parsed.countRequested()) {
         selectClause = finalVertexAlias + "." + idCol;
      } else if (!parsed.projections().isEmpty()) {
         StringJoiner projectionSelect = new StringJoiner(", ");

         for(ProjectionField projection : parsed.projections()) {
            if (projection.kind() != GremlinSqlTranslator.ProjectionKind.EDGE_PROPERTY) {
               throw new IllegalArgumentException("both() projection currently supports by('property') only");
            }

            String mappedColumn = this.mapVertexProperty(startVertexMapping, projection.property());
            projectionSelect.add(finalVertexAlias + "." + mappedColumn + " AS " + this.quoteAlias(projection.alias()));
         }

         selectClause = projectionSelect.toString();
      } else if (parsed.pathSeen() && !parsed.pathByProperties().isEmpty()) {
         selectClause = this.buildPathSelectClause(parsed.pathByProperties(), hopCount, mappingConfig, startVertexMapping, parsed.hops()).substring("SELECT ".length());
      } else if (parsed.valueProperty() != null) {
         String mappedColumn = this.mapVertexProperty(startVertexMapping, parsed.valueProperty());
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
            if (parsed.whereClause().kind() == GremlinSqlTranslator.WhereKind.EDGE_EXISTS) {
               this.appendEdgeExistsPredicate(startSubquery, sharedStartParams, parsed.whereClause(), mappingConfig, startVertexMapping, null, !parsed.filters().isEmpty());
            } else if (parsed.whereClause().kind() == GremlinSqlTranslator.WhereKind.OUT_NEIGHBOR_HAS || parsed.whereClause().kind() == GremlinSqlTranslator.WhereKind.IN_NEIGHBOR_HAS) {
               this.appendNeighborHasPredicate(startSubquery, sharedStartParams, parsed.whereClause(), mappingConfig, startVertexMapping, null, !parsed.filters().isEmpty());
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

         if (parsed.whereClause() != null && parsed.whereClause().kind() == GremlinSqlTranslator.WhereKind.EDGE_EXISTS) {
            this.appendEdgeExistsPredicate(outBranch, outParams, parsed.whereClause(), mappingConfig, startVertexMapping, "v0", hasWhere);
            this.appendEdgeExistsPredicate(inBranch, inParams, parsed.whereClause(), mappingConfig, startVertexMapping, "v0", hasWhere);
         }
      }

      if (parsed.simplePathRequested() && hopCount > 0) {
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
      } else if (!parsed.projections().isEmpty()) {
         return this.buildEdgeProjectionSql(parsed, mappingConfig, edgeMapping);
      } else if (!parsed.hops().isEmpty()) {
         return this.buildEdgeToVertexSql(parsed, mappingConfig, edgeMapping);
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
         appendLimit(sql, parsed.limit() != null ? parsed.limit() : parsed.preHopLimit());
         return new TranslationResult(sql.toString(), params);
      }
   }

   private TranslationResult buildEdgeToVertexSql(ParsedTraversal parsed, MappingConfig mappingConfig, EdgeMapping edgeMapping) {
      if (parsed.hops().size() != 1) {
         throw new IllegalArgumentException("Only a single outV() or inV() hop is supported after g.E()");
      } else {
         HopStep hop = parsed.hops().get(0);
         if (!"outV".equals(hop.direction()) && !"inV".equals(hop.direction())) {
            throw new IllegalArgumentException("Only outV() or inV() hops are supported after g.E()");
         } else {
            VertexMapping vertexMapping = Optional.ofNullable(this.resolveVertexMappingForEdgeProjection(mappingConfig)).orElseThrow(() -> new IllegalArgumentException("outV()/inV() traversal requires exactly one vertex mapping to be present"));
            String joinColumn = "outV".equals(hop.direction()) ? edgeMapping.outColumn() : edgeMapping.inColumn();
            List<Object> params = new ArrayList<>();
            StringBuilder sql = (new StringBuilder("SELECT DISTINCT v.*")).append(" FROM ").append(vertexMapping.table()).append(" v").append(" JOIN ").append(edgeMapping.table()).append(" e").append(" ON v.").append(vertexMapping.idColumn()).append(" = e.").append(joinColumn);
            this.appendWhereClauseForEdgeAlias(sql, params, parsed.filters(), edgeMapping, "e");
            appendOrderBy(sql, parsed.orderByProperty(), parsed.orderDirection());
            appendLimit(sql, parsed.limit() != null ? parsed.limit() : parsed.preHopLimit());
            return new TranslationResult(sql.toString(), params);
         }
      }
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
            if (projection.kind() == GremlinSqlTranslator.ProjectionKind.EDGE_PROPERTY) {
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
                     if (neighborMapping == null) {
                        neighborMapping = vertexMapping;
                     }

                     String anchorCol = isOut ? edgeMapping.outColumn() : edgeMapping.inColumn();
                     String targetCol = isOut ? edgeMapping.inColumn() : edgeMapping.outColumn();
                     String mappedCol = this.mapVertexProperty(neighborMapping, neighborProp);
                     String subqAlias = "_nje" + neighborJoinIdx;
                     String vAlias = "_njv" + neighborJoinIdx;
                     ++neighborJoinIdx;
                     String subq = "(SELECT STRING_AGG(" + vAlias + "." + mappedCol + ", ',') FROM " + edgeMapping.table() + " " + subqAlias + " JOIN " + neighborMapping.table() + " " + vAlias + " ON " + vAlias + "." + neighborMapping.idColumn() + " = " + subqAlias + "." + targetCol + " WHERE " + subqAlias + "." + anchorCol + " = v." + vertexMapping.idColumn() + ")";
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
         if (parsed.whereClause() != null && parsed.whereClause().kind() == GremlinSqlTranslator.WhereKind.EDGE_EXISTS) {
            this.appendEdgeExistsPredicate(baseSql, params, parsed.whereClause(), mappingConfig, vertexMapping, "v", !parsed.filters().isEmpty());
         } else if (parsed.whereClause() != null && (parsed.whereClause().kind() == GremlinSqlTranslator.WhereKind.OUT_NEIGHBOR_HAS || parsed.whereClause().kind() == GremlinSqlTranslator.WhereKind.IN_NEIGHBOR_HAS)) {
            this.appendNeighborHasPredicate(baseSql, params, parsed.whereClause(), mappingConfig, vertexMapping, "v", !parsed.filters().isEmpty());
         }

         if (parsed.whereClause() != null) {
            if (parsed.whereClause().kind() != GremlinSqlTranslator.WhereKind.PROJECT_GT && parsed.whereClause().kind() != GremlinSqlTranslator.WhereKind.PROJECT_GTE) {
               if (parsed.whereClause().kind() != GremlinSqlTranslator.WhereKind.EDGE_EXISTS && parsed.whereClause().kind() != GremlinSqlTranslator.WhereKind.OUT_NEIGHBOR_HAS && parsed.whereClause().kind() != GremlinSqlTranslator.WhereKind.IN_NEIGHBOR_HAS) {
                  throw new IllegalArgumentException("Only where(select('alias').is(gt/gte(n))) or where(outE/inE/bothE(...)) or where(out/in('label').has(...)) is supported with project(...)");
               } else {
                  appendOrderBy(baseSql, parsed.orderByProperty(), parsed.orderDirection());
                  Integer effectiveLimit = parsed.limit() != null ? parsed.limit() : parsed.preHopLimit();
                  appendLimit(baseSql, effectiveLimit);
                  return new TranslationResult(baseSql.toString(), params);
               }
            } else {
               String numericLiteral = this.sanitizeNumericLiteral(parsed.whereClause().right());
               String operator = parsed.whereClause().kind() == GremlinSqlTranslator.WhereKind.PROJECT_GTE ? ">=" : ">";
               StringBuilder wrapped = (new StringBuilder("SELECT * FROM (")).append(baseSql).append(") p WHERE p.").append(this.quoteAlias(parsed.whereClause().left())).append(" ").append(operator).append(" ").append(numericLiteral);
               Integer effectiveLimit = parsed.limit() != null ? parsed.limit() : parsed.preHopLimit();
               appendOrderBy(wrapped, parsed.orderByProperty(), parsed.orderDirection());
               appendLimit(wrapped, effectiveLimit);
               return new TranslationResult(wrapped.toString(), params);
            }
         } else {
            Integer effectiveLimit = parsed.limit() != null ? parsed.limit() : parsed.preHopLimit();
            appendOrderBy(baseSql, parsed.orderByProperty(), parsed.orderDirection());
            appendLimit(baseSql, effectiveLimit);
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

   private void appendEdgeExistsPredicate(StringBuilder sql, List<Object> params, WhereClause whereClause, MappingConfig mappingConfig, VertexMapping vertexMapping, String vertexAlias, boolean hasExistingWhere) {
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

      StringBuilder existsSql = (new StringBuilder("EXISTS (SELECT 1 FROM ")).append(edgeMapping.table()).append(' ').append(edgeAlias).append(" WHERE ").append(correlation);

      for(HasFilter filter : whereClause.filters()) {
         String column = this.mapEdgeFilterProperty(edgeMapping, filter.property());
         existsSql.append(" AND ").append(edgeAlias).append('.').append(column).append(" ").append(filter.operator()).append(" ?");
         params.add(filter.value());
      }

      existsSql.append(")");
      sql.append(hasExistingWhere ? " AND " : " WHERE ").append(existsSql);
   }

   private void appendNeighborHasPredicate(StringBuilder sql, List<Object> params, WhereClause whereClause, MappingConfig mappingConfig, VertexMapping vertexMapping, String vertexAlias, boolean hasExistingWhere) {
      EdgeMapping edgeMapping = this.resolveEdgeMapping(whereClause.right(), mappingConfig);
      boolean isOut = "out".equals(whereClause.left());
      String anchorCol = isOut ? edgeMapping.outColumn() : edgeMapping.inColumn();
      String targetCol = isOut ? edgeMapping.inColumn() : edgeMapping.outColumn();
      VertexMapping neighborVertexMapping = this.resolveTargetVertexMapping(mappingConfig, edgeMapping, isOut);
      if (neighborVertexMapping == null) {
         neighborVertexMapping = vertexMapping;
      }

      String idRef = vertexAlias == null ? vertexMapping.idColumn() : vertexAlias + "." + vertexMapping.idColumn();
      StringBuilder existsSql = (new StringBuilder("EXISTS (SELECT 1 FROM ")).append(edgeMapping.table()).append(" _we").append(" JOIN ").append(neighborVertexMapping.table()).append(" _wv").append(" ON _wv.").append(neighborVertexMapping.idColumn()).append(" = _we.").append(targetCol).append(" WHERE _we.").append(anchorCol).append(" = ").append(idRef);

      for(HasFilter filter : whereClause.filters()) {
         String column = this.mapVertexProperty(neighborVertexMapping, filter.property());
         existsSql.append(" AND _wv.").append(column).append(" ").append(filter.operator()).append(" ?");
         params.add(filter.value());
      }

      existsSql.append(")");
      sql.append(hasExistingWhere ? " AND " : " WHERE ").append(existsSql);
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
            whereJoiner.add(qualifiedColumn + " " + filter.operator() + " ?");
            params.add(filter.value());
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
            whereJoiner.add(qualifiedColumn + " " + filter.operator() + " ?");
            params.add(filter.value());
         }

         sql.append(" WHERE ").append(whereJoiner);
      }
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
            boolean outDirection = !"in".equals(hop.direction());
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
            VertexMapping keyVertexMapping;
            if (aliasHopIndex == 0) {
               keyVertexMapping = startVertexMapping;
            } else {
               keyVertexMapping = startVertexMapping;

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

   private ParsedTraversal parseSteps(List<GremlinStep> stepNodes, String rootIdFilter) {
      String label = null;
      String valuesProperty = null;
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
      if (rootIdFilter != null) {
         filters.add(new HasFilter("id", rootIdFilter));
      }

      for(GremlinStep stepNode : stepNodes) {
         String stepName = stepNode.name();
         List<String> args = stepNode.args();
         String rawStepArgs = stepNode.rawArgs() == null ? "" : stepNode.rawArgs().trim();
         if (whereByPending && !"by".equals(stepName)) {
            whereByPending = false;
         }

         switch (stepName) {
            case "hasLabel":
               ensureArgCount(stepName, args, 1);
               label = this.unquote(args.get(0));
               break;
            case "has":
               ensureArgCount(stepName, args, 2);
               String hasProp = this.unquote(args.get(0));
               String hasVal = args.get(1).trim();
               HasFilter filter = this.parseHasArgument(hasProp, hasVal);
               filters.add(filter);
               break;
            case "values":
               ensureArgCount(stepName, args, 1);
               valuesProperty = this.unquote(args.get(0));
               break;
            case "out":
               if (args.isEmpty()) {
                  throw new IllegalArgumentException("out expects at least 1 argument");
               }

               hopsSeen = true;
               hops.add(new HopStep("out", args.stream().map(this::unquote).toList()));
               break;
            case "in":
               if (args.isEmpty()) {
                  throw new IllegalArgumentException("in expects at least 1 argument");
               }

               hopsSeen = true;
               hops.add(new HopStep("in", args.stream().map(this::unquote).toList()));
               break;
            case "outV":
               ensureArgCount(stepName, args, 0);
               hopsSeen = true;
               if (this.normalizeEndpointHop("outV", hops)) {
                  hops.add(new HopStep("outV", List.of()));
               }
               break;
            case "inV":
               ensureArgCount(stepName, args, 0);
               hopsSeen = true;
               if (this.normalizeEndpointHop("inV", hops)) {
                  hops.add(new HopStep("inV", List.of()));
               }
               break;
            case "both":
               if (args.size() > 1) {
                  throw new IllegalArgumentException("both expects 0 or 1 argument(s)");
               }

               hopsSeen = true;
               List<String> labels = args.isEmpty() ? List.of() : List.of(this.unquote(args.get(0)));
               hops.add(new HopStep("both", labels));
               break;
            case "repeat":
               if (rawStepArgs.isBlank()) {
                  throw new IllegalArgumentException("repeat() requires an argument");
               }

               hopsSeen = true;
               pendingRepeatHop = this.parseRepeatHop(rawStepArgs);
               break;
            case "simplePath":
               ensureArgCount(stepName, args, 0);
               simplePathRequested = true;
               break;
            case "times":
               if (pendingRepeatHop == null) {
                  throw new IllegalArgumentException("times() must follow repeat(out(...)), repeat(in(...)), or repeat(both(...))");
               }

               ensureArgCount(stepName, args, 1);

               int repeatCount;
               try {
                  repeatCount = Integer.parseInt(args.get(0).trim());
               } catch (NumberFormatException var43) {
                  throw new IllegalArgumentException("times() must be numeric");
               }

               if (repeatCount < 0) {
                  throw new IllegalArgumentException("times() must be >= 0");
               }

               if (pendingRepeatHop.simplePathDetected()) {
                  simplePathRequested = true;
               }

               for(int i = 0; i < repeatCount; ++i) {
                  hops.add(pendingRepeatHop.hop());
               }

               pendingRepeatHop = null;
               break;
            case "limit":
               if (!args.isEmpty() && args.size() <= 2) {
                  String limitArg = args.get(args.size() - 1).trim();

                  int parsedLimit;
                  try {
                     parsedLimit = Integer.parseInt(limitArg);
                  } catch (NumberFormatException var42) {
                     throw new IllegalArgumentException("limit() must be numeric");
                  }

                  if (!hopsSeen) {
                     preHopLimit = parsedLimit;
                  } else {
                     limit = parsedLimit;
                  }
                  break;
               }

               throw new IllegalArgumentException("limit expects 1 or 2 argument(s)");
            case "count":
               ensureArgCount(stepName, args, 0);
               countRequested = true;
               break;
            case "sum":
               ensureArgCount(stepName, args, 0);
               sumRequested = true;
               break;
            case "mean":
               ensureArgCount(stepName, args, 0);
               meanRequested = true;
               break;
            case "groupCount":
               ensureArgCount(stepName, args, 0);
               if (groupCountSeen) {
                  throw new IllegalArgumentException("Only a single groupCount() step is supported");
               }

               groupCountSeen = true;
               break;
            case "project":
               if (args.isEmpty()) {
                  throw new IllegalArgumentException("project expects at least 1 argument");
               }

               if (!projectAliases.isEmpty()) {
                  throw new IllegalArgumentException("Only a single project(...) step is supported");
               }

               for(String arg : args) {
                  String a = this.unquote(arg);
                  if (a.isBlank()) {
                     throw new IllegalArgumentException("project aliases must be non-empty");
                  }

                  projectAliases.add(a);
               }
               break;
            case "outE":
               if (args.size() > 1) {
                  throw new IllegalArgumentException("outE expects 0 or 1 argument(s)");
               }

               hopsSeen = true;
               hops.add(new HopStep("outE", args.isEmpty() ? List.of() : List.of(this.unquote(args.get(0)))));
               break;
            case "inE":
               if (args.size() > 1) {
                  throw new IllegalArgumentException("inE expects 0 or 1 argument(s)");
               }

               hopsSeen = true;
               hops.add(new HopStep("inE", args.isEmpty() ? List.of() : List.of(this.unquote(args.get(0)))));
               break;
            case "as":
               ensureArgCount(stepName, args, 1);
               String aliasName = this.unquote(args.get(0));
               if (aliasName.isBlank()) {
                  throw new IllegalArgumentException("as() label must be non-empty");
               }

               asAliases.add(new AsAlias(aliasName, hops.size()));
               break;
            case "select":
               if (args.isEmpty()) {
                  throw new IllegalArgumentException("select expects at least 1 argument");
               }

               if (selectSeen) {
                  throw new IllegalArgumentException("Only a single select(...) step is supported");
               }

               selectSeen = true;

               for(String arg : args) {
                  selectAliases.add(this.unquote(arg));
               }
               break;
            case "where":
               if (whereClause != null) {
                  throw new IllegalArgumentException("Only a single where() step is supported");
               }

               whereClause = this.parseWhere(rawStepArgs);
               whereByPending = whereClause.kind() == GremlinSqlTranslator.WhereKind.NEQ_ALIAS;
               break;
            case "dedup":
               ensureArgCount(stepName, args, 0);
               dedupRequested = true;
               break;
            case "path":
               ensureArgCount(stepName, args, 0);
               pathSeen = true;
               break;
            case "by":
               if (args.isEmpty()) {
                  throw new IllegalArgumentException("by(...) expects at least 1 argument");
               }

               if (whereByPending) {
                  ensureArgCount(stepName, args, 1);
                  whereByProperty = this.unquote(args.get(0));
                  whereByPending = false;
                  break;
               }

               if (selectSeen) {
                  if (selectByExpressions.size() >= selectAliases.size()) {
                     throw new IllegalArgumentException("select(...) has more by(...) modulators than selected aliases");
                  }

                  selectByExpressions.add(rawStepArgs);
               } else if (orderSeen && orderByProperty == null && !orderByCountDesc) {
                  List<String> byArgsTrimmed = args.stream().map(String::trim).toList();
                  if (byArgsTrimmed.size() == 2 && ("values".equals(byArgsTrimmed.get(0)) || "__.values()".equals(byArgsTrimmed.get(0)))) {
                     String dirToken = byArgsTrimmed.get(1).toLowerCase();
                     boolean isDesc = dirToken.equals("desc") || dirToken.equals("order.desc");
                     orderByCountDesc = true;
                     orderDirection = isDesc ? "DESC" : "ASC";
                  } else {
                     if (rawStepArgs.contains("select(") && rawStepArgs.contains("Order.")) {
                        int selectStart = rawStepArgs.indexOf("select(") + 7;
                        int selectEnd = rawStepArgs.indexOf(")", selectStart);
                        orderByProperty = this.unquote(rawStepArgs.substring(selectStart, selectEnd));
                        orderDirection = rawStepArgs.contains("Order.desc") ? "DESC" : "ASC";
                        break;
                     }

                     orderByProperty = this.unquote(rawStepArgs);
                     orderDirection = "ASC";
                  }
               } else if (groupCountSeen && groupCountProperty == null && groupCountKeySpec == null) {
                  GroupCountKeySpec parsed = this.tryParseGroupCountSelectKey(rawStepArgs);
                  if (parsed != null) {
                     groupCountKeySpec = parsed;
                  } else {
                     ensureArgCount(stepName, args, 1);
                     groupCountProperty = this.unquote(args.get(0));
                     if (groupCountProperty.isBlank()) {
                        throw new IllegalArgumentException("by(...) property must be non-empty");
                     }
                  }
               } else if (!projectAliases.isEmpty()) {
                  if (byExpressions.size() >= projectAliases.size()) {
                     throw new IllegalArgumentException("project(...) has more by(...) modulators than projected fields");
                  }

                  byExpressions.add(rawStepArgs);
               } else {
                  if (!pathSeen) {
                     throw new IllegalArgumentException("by(...) is only supported after project(...), groupCount(...), or order(...)");
                  }

                  ensureArgCount(stepName, args, 1);
                  pathByProperties.add(this.unquote(args.get(0)));
               }
               break;
            case "order":
               if (args.size() > 1) {
                  throw new IllegalArgumentException("order expects 0 or 1 argument(s)");
               }

               if (orderSeen) {
                  throw new IllegalArgumentException("Only a single order() step is supported");
               }

               orderSeen = true;
               break;
            default:
               throw new IllegalArgumentException("Unsupported step: " + stepName);
         }
      }

      if (pendingRepeatHop != null) {
         throw new IllegalArgumentException("repeat(...) must be followed by times(n)");
      } else if (!projectAliases.isEmpty() && byExpressions.size() != projectAliases.size()) {
         throw new IllegalArgumentException("project(...) requires one by(...) modulator per projected field");
      } else {
         if (pathSeen && pathByProperties.size() == 1 && valuesProperty == null) {
            valuesProperty = pathByProperties.get(0);
         }

         List<ProjectionField> projections = new ArrayList<>();

         for(int i = 0; i < projectAliases.size(); ++i) {
            projections.add(this.parseProjection(projectAliases.get(i), byExpressions.get(i)));
         }

         List<SelectField> selectFields = new ArrayList<>();
         String sharedSelectBy = null;
         if (selectAliases.size() > 1 && selectByExpressions.size() == 1) {
            sharedSelectBy = this.unquote(selectByExpressions.get(0));
         }

         for(int i = 0; i < selectAliases.size(); ++i) {
            String selectAlias = selectAliases.get(i);
            String property = sharedSelectBy != null ? sharedSelectBy : (i < selectByExpressions.size() ? this.unquote(selectByExpressions.get(i)) : null);
            selectFields.add(new SelectField(selectAlias, property));
         }

         if ((sumRequested || meanRequested) && valuesProperty == null) {
            if (selectFields.size() == 1) {
               SelectField sf = selectFields.get(0);
               if (sf.property() != null && !sf.property().isBlank()) {
                  valuesProperty = sf.property();
               } else {
                  for(ProjectionField pf : projections) {
                     if (pf.alias().equals(sf.alias()) && pf.kind() == GremlinSqlTranslator.ProjectionKind.EDGE_PROPERTY) {
                        valuesProperty = pf.property();
                        break;
                     }
                  }
               }
            }

            if (valuesProperty == null) {
               throw new IllegalArgumentException((meanRequested ? "mean()" : "sum()") + " requires values('property') before aggregation");
            }
         }

         return new ParsedTraversal(label, filters, valuesProperty, limit, preHopLimit, hops, countRequested, sumRequested, meanRequested, projections, groupCountProperty, orderByProperty, orderDirection, asAliases, selectFields, whereClause, dedupRequested, pathSeen, pathByProperties, whereByProperty, simplePathRequested, groupCountKeySpec, orderByCountDesc);
      }
   }

   private ProjectionField parseProjection(String alias, String byExpression) {
      String expression = byExpression == null ? "" : byExpression.trim();
      Matcher degreeMatcher = EDGE_DEGREE_PATTERN.matcher(expression);
      if (degreeMatcher.matches()) {
         String direction = degreeMatcher.group(1).equals("outE") ? "out" : "in";
         String edgeLabel = degreeMatcher.group(2);
         String hasChain = degreeMatcher.group(3);
         List<HasFilter> degreeFilters = this.parseHasChain(hasChain);
         String encodedProperty = direction + ":" + edgeLabel;
         if (!degreeFilters.isEmpty()) {
            StringBuilder encoded = new StringBuilder(encodedProperty);

            for(HasFilter f : degreeFilters) {
               encoded.append(":").append(f.property()).append("=").append(f.value());
            }

            encodedProperty = encoded.toString();
         }

         return new ProjectionField(alias, GremlinSqlTranslator.ProjectionKind.EDGE_DEGREE, encodedProperty);
      } else {
         Matcher vertexCountMatcher = VERTEX_COUNT_PATTERN.matcher(expression);
         if (!vertexCountMatcher.matches()) {
            Matcher neighborValuesMatcher = NEIGHBOR_VALUES_FOLD_PATTERN.matcher(expression);
            if (neighborValuesMatcher.matches()) {
               String direction = neighborValuesMatcher.group(1);
               String labelsRaw = neighborValuesMatcher.group(2);
               String prop = neighborValuesMatcher.group(3);
               List<String> labelList = this.splitArgs(labelsRaw).stream().map(this::unquote).toList();
               String edgeLabel = labelList.get(0);
               String encoded = direction + ":" + edgeLabel + ":" + prop;
               ProjectionKind kind = "out".equals(direction) ? GremlinSqlTranslator.ProjectionKind.OUT_NEIGHBOR_PROPERTY : GremlinSqlTranslator.ProjectionKind.IN_NEIGHBOR_PROPERTY;
               return new ProjectionField(alias, kind, encoded);
            } else {
               ChooseProjectionSpec chooseSpec = this.parseChooseProjection(expression);
               if (chooseSpec != null) {
                  return new ProjectionField(alias, GremlinSqlTranslator.ProjectionKind.CHOOSE_VALUES_IS_CONSTANT, expression);
               }

               Matcher endpointMatcher = ENDPOINT_VALUES_PATTERN.matcher(expression);
               if (endpointMatcher.matches()) {
                  String endpoint = endpointMatcher.group(1);
                  String property = this.unquote(endpointMatcher.group(2));
                  if (property.isBlank()) {
                     throw new IllegalArgumentException("by(outV().values(...)) and by(inV().values(...)) require a property name");
                  } else {
                     ProjectionKind kind = "outV".equals(endpoint) ? GremlinSqlTranslator.ProjectionKind.OUT_VERTEX_PROPERTY : GremlinSqlTranslator.ProjectionKind.IN_VERTEX_PROPERTY;
                     return new ProjectionField(alias, kind, property);
                  }
               } else if (expression.contains("(")) {
                  throw new IllegalArgumentException("Unsupported by() projection expression: " + expression);
               } else {
                  String property = this.unquote(expression);
                  if (property.isBlank()) {
                     throw new IllegalArgumentException("by(...) property must be non-empty");
                  } else {
                     return new ProjectionField(alias, GremlinSqlTranslator.ProjectionKind.EDGE_PROPERTY, property);
                  }
               }
            }
         } else {
            String direction = vertexCountMatcher.group(1);
            String edgeLabel = vertexCountMatcher.group(2);
            String hasChain = vertexCountMatcher.group(3);
            List<HasFilter> vcFilters = this.parseHasChain(hasChain);
            StringBuilder encoded = (new StringBuilder(direction)).append(":").append(edgeLabel);

            for(HasFilter f : vcFilters) {
               encoded.append(":").append(f.property()).append("=").append(f.value());
            }

            ProjectionKind kind = "out".equals(direction) ? GremlinSqlTranslator.ProjectionKind.OUT_VERTEX_COUNT : GremlinSqlTranslator.ProjectionKind.IN_VERTEX_COUNT;
            return new ProjectionField(alias, kind, encoded.toString());
         }
      }
   }

   private VertexMapping resolveVertexMappingForEdgeProjection(MappingConfig mappingConfig) {
      return mappingConfig.vertices().size() == 1 ? mappingConfig.vertices().values().iterator().next() : null;
   }

   private VertexMapping resolveVertexMappingByProperties(MappingConfig mappingConfig, List<String> propertyNames) {
      if (propertyNames.isEmpty()) {
         return null;
      } else {
         for(VertexMapping vm : mappingConfig.vertices().values()) {
            if (propertyNames.stream().allMatch((p) -> vm.properties().containsKey(p))) {
               return vm;
            }
         }

         return null;
      }
   }

   private VertexMapping resolveTargetVertexMapping(MappingConfig mappingConfig, EdgeMapping edgeMapping, boolean outDirection) {
      if (mappingConfig.vertices().size() == 1) {
         return mappingConfig.vertices().values().iterator().next();
      } else {
         String targetCol = outDirection ? edgeMapping.inColumn() : edgeMapping.outColumn();
         String stem = targetCol.replaceAll("_id$", "").replaceAll("^.*_", "");

         for(Map.Entry<String, VertexMapping> entry : mappingConfig.vertices().entrySet()) {
            String tableStem = entry.getValue().table().replaceAll("^[a-z]+_", "");
            if (tableStem.equals(stem) || tableStem.startsWith(stem)) {
               return entry.getValue();
            }
         }

         for(Map.Entry<String, VertexMapping> entry : mappingConfig.vertices().entrySet()) {
            if (entry.getKey().equalsIgnoreCase(stem)) {
               return entry.getValue();
            }
         }

         for(VertexMapping vm : mappingConfig.vertices().values()) {
            if (targetCol.equals(vm.idColumn())) {
               return vm;
            }
         }

         return null;
      }
   }

   private String quoteAlias(String alias) {
      String escaped = alias.replace("\"", "\"\"");
      return "\"" + escaped + "\"";
   }

   private List<HasFilter> parseHasChain(String hasChain) {
      List<HasFilter> filters = new ArrayList<>();
      if (hasChain != null && !hasChain.isBlank()) {
         Matcher m = HAS_CHAIN_ITEM.matcher(hasChain);

         while(m.find()) {
            filters.add(new HasFilter(m.group(1), m.group(2)));
         }

         return filters;
      } else {
         return filters;
      }
   }

   private RepeatHopResult parseRepeatHop(String repeatBodyRaw) {
      String text = repeatBodyRaw == null ? "" : repeatBodyRaw.trim();
      boolean hasSimplePath = text.endsWith(".simplePath()");
      if (hasSimplePath) {
         text = text.substring(0, text.length() - ".simplePath()".length()).trim();
      }

      if (!text.endsWith(")")) {
         throw new IllegalArgumentException("Only repeat(out(...)), repeat(in(...)), or repeat(both(...)) is supported");
      } else {
         String direction;
         int openParenIndex;
         if (text.startsWith("out(")) {
            direction = "out";
            openParenIndex = 3;
         } else if (text.startsWith("in(")) {
            direction = "in";
            openParenIndex = 2;
         } else {
            if (!text.startsWith("both(")) {
               throw new IllegalArgumentException("Only repeat(out(...)), repeat(in(...)), or repeat(both(...)) is supported");
            }

            direction = "both";
            openParenIndex = 4;
         }

         String inner = text.substring(openParenIndex + 1, text.length() - 1).trim();
         if (inner.isEmpty()) {
            return new RepeatHopResult(new HopStep(direction, List.of()), hasSimplePath);
         } else {
            List<String> labels = this.splitArgs(inner).stream().map(this::unquote).toList();
            return new RepeatHopResult(new HopStep(direction, labels), hasSimplePath);
         }
      }
   }

   private boolean normalizeEndpointHop(String endpointStep, List<HopStep> hops) {
      if (hops.isEmpty()) {
         return true;
      } else {
         if (hops.size() > 1) {
            HopStep beforePrevious = hops.get(hops.size() - 2);
            if ("outV".equals(beforePrevious.direction()) || "inV".equals(beforePrevious.direction())) {
               return true;
            }
         }

         HopStep previous = hops.get(hops.size() - 1);
         if (!"outE".equals(previous.direction()) && !"inE".equals(previous.direction())) {
            return true;
         } else {
            hops.remove(hops.size() - 1);
            String normalized;
            if ("outE".equals(previous.direction())) {
               normalized = "inV".equals(endpointStep) ? "out" : "in";
            } else {
               normalized = "inV".equals(endpointStep) ? "in" : "out";
            }

            hops.add(new HopStep(normalized, previous.labels()));
            return false;
         }
      }
   }

   private WhereClause parseWhere(String rawWhere) {
      Matcher neqMatcher = WHERE_NEQ_PATTERN.matcher(rawWhere);
      if (neqMatcher.matches()) {
         return new WhereClause(GremlinSqlTranslator.WhereKind.NEQ_ALIAS, neqMatcher.group(1), neqMatcher.group(2), List.of());
      } else {
         Matcher eqAliasMatcher = WHERE_EQ_ALIAS_PATTERN.matcher(rawWhere);
         if (eqAliasMatcher.matches()) {
            return new WhereClause(GremlinSqlTranslator.WhereKind.EQ_ALIAS, eqAliasMatcher.group(1), null, List.of());
         } else {
            Matcher gtMatcher = WHERE_SELECT_IS_GT_PATTERN.matcher(rawWhere);
            if (gtMatcher.matches()) {
               String alias = gtMatcher.group(1);
               String value = gtMatcher.group(2).trim();
               boolean isGte = rawWhere.contains(".is(gte(");
               return new WhereClause(isGte ? GremlinSqlTranslator.WhereKind.PROJECT_GTE : GremlinSqlTranslator.WhereKind.PROJECT_GT, alias, value, List.of());
            } else {
               Matcher edgeExistsMatcher = WHERE_EDGE_EXISTS_PATTERN.matcher(rawWhere);
               if (edgeExistsMatcher.matches()) {
                  String direction = edgeExistsMatcher.group(1);
                  String edgeLabel = edgeExistsMatcher.group(2);
                  String hasChain = edgeExistsMatcher.group(3);
                  return new WhereClause(GremlinSqlTranslator.WhereKind.EDGE_EXISTS, direction, edgeLabel, this.parseHasChain(hasChain));
               } else {
                  Matcher neighborMatcher = WHERE_OUT_NEIGHBOR_HAS_PATTERN.matcher(rawWhere);
                  if (neighborMatcher.matches()) {
                     String direction = neighborMatcher.group(1);
                     String edgeLabel = neighborMatcher.group(2);
                     String hasChain = neighborMatcher.group(3);
                     WhereKind kind = "out".equals(direction) ? GremlinSqlTranslator.WhereKind.OUT_NEIGHBOR_HAS : GremlinSqlTranslator.WhereKind.IN_NEIGHBOR_HAS;
                     return new WhereClause(kind, direction, edgeLabel, this.parseHasChain(hasChain));
                  } else {
                     throw new IllegalArgumentException("Unsupported where() predicate: " + rawWhere);
                  }
               }
            }
         }
      }
   }

   private static void ensureArgCount(String step, List<String> args, int expected) {
      if (args.size() != expected) {
         throw new IllegalArgumentException(step + " expects " + expected + " argument(s)");
      }
   }

   private HasFilter parseHasArgument(String property, String rawValue) {
      String trimmed = rawValue.trim();
      Matcher m = HAS_PREDICATE_PATTERN.matcher(trimmed);
      if (m.matches()) {
         String predicate = m.group(1);
         String innerVal = this.unquote(m.group(2).trim());
         String var10000;
         switch (predicate) {
            case "gt" -> var10000 = ">";
            case "gte" -> var10000 = ">=";
            case "lt" -> var10000 = "<";
            case "lte" -> var10000 = "<=";
            case "neq" -> var10000 = "!=";
            case "eq" -> var10000 = "=";
            default -> var10000 = "=";
         }

         String operator = var10000;
         return new HasFilter(property, innerVal, operator);
      } else {
         return new HasFilter(property, this.unquote(trimmed));
      }
   }

   private GroupCountKeySpec tryParseGroupCountSelectKey(String rawExpr) {
      if (rawExpr != null && !rawExpr.isBlank()) {
         Matcher m = GROUP_COUNT_SELECT_BY_PATTERN.matcher(rawExpr.trim());
         return m.matches() ? new GroupCountKeySpec(m.group(1), m.group(2)) : null;
      } else {
         return null;
      }
   }

   private List<String> splitArgs(String argsText) {
      List<String> args = new ArrayList<>();
      StringBuilder current = new StringBuilder();
      boolean inSingleQuote = false;
      boolean inDoubleQuote = false;

      for(int i = 0; i < argsText.length(); ++i) {
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
      return (!trimmed.startsWith("\"") || !trimmed.endsWith("\"")) && (!trimmed.startsWith("'") || !trimmed.endsWith("'")) ? trimmed : trimmed.substring(1, trimmed.length() - 1);
   }

   private record RepeatHopResult(HopStep hop, boolean simplePathDetected) {
   }

   private record HopStep(String direction, List<String> labels) {
      HopStep(String direction, String singleLabel) {
         this(direction, singleLabel == null ? List.of() : List.of(singleLabel));
      }

      String singleLabel() {
         if (this.labels.isEmpty()) {
            return null;
         } else if (this.labels.size() > 1) {
            throw new IllegalStateException("singleLabel() called on multi-label HopStep: " + this.labels);
         } else {
            return this.labels.get(0);
         }
      }
   }

   private record ParsedTraversal(String label, List<HasFilter> filters, String valueProperty, Integer limit, Integer preHopLimit, List<HopStep> hops, boolean countRequested, boolean sumRequested, boolean meanRequested, List<ProjectionField> projections, String groupCountProperty, String orderByProperty, String orderDirection, List<AsAlias> asAliases, List<SelectField> selectFields, WhereClause whereClause, boolean dedupRequested, boolean pathSeen, List<String> pathByProperties, String whereByProperty, boolean simplePathRequested, GroupCountKeySpec groupCountKeySpec, boolean orderByCountDesc) {
   }

   private record GroupCountKeySpec(String alias, String property) {
   }

   private record ChooseProjectionSpec(String property, String predicate, String predicateValue, String trueConstant, String falseConstant) {
   }

   private enum ProjectionKind {
      EDGE_PROPERTY,
      OUT_VERTEX_PROPERTY,
      IN_VERTEX_PROPERTY,
      EDGE_DEGREE,
      OUT_VERTEX_COUNT,
      IN_VERTEX_COUNT,
      OUT_NEIGHBOR_PROPERTY,
      IN_NEIGHBOR_PROPERTY,
      CHOOSE_VALUES_IS_CONSTANT;

      ProjectionKind() {
      }

   }

   private record ProjectionField(String alias, ProjectionKind kind, String property) {
   }

   private record AsAlias(String label, int hopIndexAfter) {
   }

   private record SelectField(String alias, String property) {
   }

   private record WhereClause(WhereKind kind, String left, String right, List<HasFilter> filters) {
   }

   private enum WhereKind {
      NEQ_ALIAS,
      EQ_ALIAS,
      PROJECT_GT,
      PROJECT_GTE,
      EDGE_EXISTS,
      OUT_NEIGHBOR_HAS,
      IN_NEIGHBOR_HAS;

      WhereKind() {
      }

   }
}
