package com.graphqueryengine.query.translate.sql.parse;

import com.graphqueryengine.query.translate.sql.HasFilter;
import com.graphqueryengine.query.translate.sql.model.AsAlias;
import com.graphqueryengine.query.translate.sql.model.ChooseProjectionSpec;
import com.graphqueryengine.query.translate.sql.model.GroupCountKeySpec;
import com.graphqueryengine.query.translate.sql.model.HopStep;
import com.graphqueryengine.query.translate.sql.model.ParsedTraversal;
import com.graphqueryengine.query.translate.sql.model.ProjectionField;
import com.graphqueryengine.query.translate.sql.model.ProjectionKind;
import com.graphqueryengine.query.translate.sql.model.RepeatHopResult;
import com.graphqueryengine.query.translate.sql.model.SelectField;
import com.graphqueryengine.query.translate.sql.model.WhereClause;
import com.graphqueryengine.query.translate.sql.model.WhereKind;
import com.graphqueryengine.query.translate.sql.constant.GremlinToken;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Stateless parser that converts a list of raw {@link com.graphqueryengine.query.parser.model.GremlinStep}
 * nodes (plus an optional root-id filter) into a fully-typed {@link ParsedTraversal}.
 *
 * <p>This class is responsible solely for understanding Gremlin step semantics and building
 * the intermediate model.  No SQL is produced here.
 */
public class GremlinStepParser {

    // ── Patterns ─────────────────────────────────────────────────────────────

    private static final Pattern ENDPOINT_VALUES_PATTERN =
            Pattern.compile("^(outV|inV)\\(\\)\\.values\\((.+)\\)$");
    private static final Pattern NEIGHBOR_VALUES_FOLD_PATTERN =
            Pattern.compile("^(out|in)\\((.+)\\)\\.values\\(['\"]([^'\"]+)['\"]\\)\\.fold\\(\\)$");
    private static final Pattern EDGE_DEGREE_PATTERN =
            Pattern.compile("^(outE|inE)\\(['\"]([^'\"]+)['\"]\\)((?:\\.has\\(['\"][^'\"]+['\"],\\s*['\"][^'\"]*['\"]\\))*)\\s*\\.count\\(\\)$");
    private static final Pattern VERTEX_COUNT_PATTERN =
            Pattern.compile("^(out|in)\\(['\"]([^'\"]+)['\"]\\)((?:\\.has\\(['\"][^'\"]+['\"],\\s*['\"][^'\"]*['\"]\\))*)\\s*\\.count\\(\\)$");
    private static final Pattern HAS_CHAIN_ITEM =
            Pattern.compile("\\.has\\('([^']+)','([^']+)'\\)");
    private static final Pattern WHERE_NEQ_PATTERN =
            Pattern.compile("^'([^']+)'\\s*,\\s*neq\\('([^']+)'\\)$");
    private static final Pattern WHERE_EQ_ALIAS_PATTERN =
            Pattern.compile("^eq\\(['\"]([^'\"]+)['\"]\\)$");
    private static final Pattern WHERE_SELECT_IS_GT_PATTERN =
            Pattern.compile("^select\\(['\"]([^'\"]+)['\"]\\)\\.is\\(gte?\\(([^)]+)\\)\\)$");
    private static final Pattern WHERE_EDGE_EXISTS_PATTERN =
            Pattern.compile("^(outE|inE|bothE)\\(['\"]([^'\"]+)['\"]\\)((?:\\.has\\(['\"][^'\"]+['\"],\\s*['\"][^'\"]*['\"]\\))*)$");
    private static final Pattern WHERE_EDGE_COUNT_IS_PATTERN =
            Pattern.compile("^(outE|inE|bothE)\\(['\"]([^'\"]+)['\"]\\)((?:\\.has\\(['\"][^'\"]+['\"],\\s*['\"][^'\"]*['\"]\\))*)\\s*\\.count\\(\\)\\.is\\((?:(eq|neq|gt|gte|lt|lte)\\(([^)]+)\\)|(-?\\d+))\\)$");
    private static final Pattern WHERE_OUT_NEIGHBOR_HAS_PATTERN =
            Pattern.compile("^(out|in)\\(['\"]([^'\"]+)['\"]\\)((?:\\.has\\(['\"][^'\"]+['\"],\\s*['\"][^'\"]*['\"]\\))*)$");
    private static final Pattern HAS_PREDICATE_PATTERN =
            Pattern.compile("^(gt|gte|lt|lte|neq|eq)\\((.+)\\)$");
    private static final Pattern CHOOSE_VALUES_IS_CONSTANT_PATTERN =
            Pattern.compile("^choose\\(\\s*values\\(['\"]([^'\"]+)['\"]\\)\\.is\\((gt|gte|lt|lte|eq|neq)\\(([^)]+)\\)\\)\\s*,\\s*constant\\(([^)]+)\\)\\s*,\\s*constant\\(([^)]+)\\)\\s*\\)$");
    private static final Pattern GROUP_COUNT_SELECT_BY_PATTERN =
            Pattern.compile("^select\\(['\"]([^'\"]+)['\"]\\)\\.by\\(['\"]([^'\"]+)['\"]\\)$");

    // ── Public entry point ────────────────────────────────────────────────────

    /**
     * Parses a list of Gremlin step nodes into a {@link ParsedTraversal}.
     *
     * @param stepNodes    the ordered list of parsed Gremlin steps
     * @param rootIdFilter an optional {@code g.V('id')} / {@code g.E('id')} root-id filter
     * @return a fully-typed intermediate representation ready for SQL generation
     */
    public ParsedTraversal parse(
            List<com.graphqueryengine.query.parser.model.GremlinStep> stepNodes,
            String rootIdFilter) {

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
        List<String> valueMapKeys = new ArrayList<>();

        if (rootIdFilter != null) filters.add(new HasFilter("id", rootIdFilter));

        for (com.graphqueryengine.query.parser.model.GremlinStep stepNode : stepNodes) {
            String stepName = stepNode.name();
            List<String> args = stepNode.args();
            String rawStepArgs = stepNode.rawArgs() == null ? "" : stepNode.rawArgs().trim();
            if (whereByPending && !"by".equals(stepName)) whereByPending = false;

            switch (stepName) {
                case "hasLabel" -> { ensureArgCount(stepName, args, 1); label = unquote(args.get(0)); }
                case "has" -> {
                    ensureArgCount(stepName, args, 2);
                    filters.add(parseHasArgument(unquote(args.get(0)), args.get(1).trim()));
                }
                case "hasId" -> {
                    ensureArgCount(stepName, args, 1);
                    filters.add(new HasFilter("id", unquote(args.get(0))));
                }
                case "hasNot" -> {
                    ensureArgCount(stepName, args, 1);
                    filters.add(new HasFilter(unquote(args.get(0)), null, "IS NULL"));
                }
                case "values" -> { ensureArgCount(stepName, args, 1); valuesProperty = unquote(args.get(0)); }
                case "is" -> {
                    ensureArgCount(stepName, args, 1);
                    if (valuesProperty == null || valuesProperty.isBlank())
                        throw new IllegalArgumentException("is(...) is only supported after values('property')");
                    filters.add(parseHasArgument(valuesProperty, args.get(0).trim()));
                }
                case "valueMap" -> {
                    valueMapRequested = true;
                    if (!args.isEmpty()) {
                        valueMapKeys = args.stream().map(this::unquote).toList();
                    }
                }
                case "out" -> {
                    if (args.isEmpty()) throw new IllegalArgumentException("out expects at least 1 argument");
                    hopsSeen = true;
                    hops.add(new HopStep(GremlinToken.OUT, args.stream().map(this::unquote).toList()));
                }
                case "in" -> {
                    if (args.isEmpty()) throw new IllegalArgumentException("in expects at least 1 argument");
                    hopsSeen = true;
                    hops.add(new HopStep(GremlinToken.IN, args.stream().map(this::unquote).toList()));
                }
                case "both" -> {
                    if (args.isEmpty()) throw new IllegalArgumentException("both expects at least 1 argument");
                    hopsSeen = true;
                    hops.add(new HopStep(GremlinToken.BOTH, args.stream().map(this::unquote).toList()));
                }
                case "outV" -> {
                    ensureArgCount(stepName, args, 0);
                    hopsSeen = true;
                    if (normalizeEndpointHop(GremlinToken.OUT_V, hops)) hops.add(new HopStep(GremlinToken.OUT_V, List.of()));
                }
                case "inV" -> {
                    ensureArgCount(stepName, args, 0);
                    hopsSeen = true;
                    if (normalizeEndpointHop(GremlinToken.IN_V, hops)) hops.add(new HopStep(GremlinToken.IN_V, List.of()));
                }
                case "bothV" -> {
                    ensureArgCount(stepName, args, 0);
                    hopsSeen = true;
                    hops.add(new HopStep(GremlinToken.BOTH_V, List.of()));
                }
                case "otherV" -> {
                    ensureArgCount(stepName, args, 0);
                    hopsSeen = true;
                    if (hops.isEmpty()) throw new IllegalArgumentException("otherV() must follow outE() or inE()");
                    HopStep previous = hops.get(hops.size() - 1);
                    if (GremlinToken.OUT_E.equals(previous.direction())) {
                        if (normalizeEndpointHop(GremlinToken.IN_V, hops)) hops.add(new HopStep(GremlinToken.IN_V, List.of()));
                    } else if (GremlinToken.IN_E.equals(previous.direction())) {
                        if (normalizeEndpointHop(GremlinToken.OUT_V, hops)) hops.add(new HopStep(GremlinToken.OUT_V, List.of()));
                    } else {
                        throw new IllegalArgumentException("otherV() must follow outE() or inE()");
                    }
                }
                case "repeat" -> {
                    if (rawStepArgs.isBlank()) throw new IllegalArgumentException("repeat() requires an argument");
                    hopsSeen = true;
                    pendingRepeatHop = parseRepeatHop(rawStepArgs);
                }
                case "simplePath" -> { ensureArgCount(stepName, args, 0); simplePathRequested = true; }
                case "times" -> {
                    if (pendingRepeatHop == null)
                        throw new IllegalArgumentException("times() must follow repeat(out(...)), repeat(in(...)), or repeat(both(...))");
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
                    if (args.isEmpty() || args.size() > 2)
                        throw new IllegalArgumentException("limit expects 1 or 2 argument(s)");
                    String limitArg = args.get(args.size() - 1).trim();
                    int parsedLimit;
                    try { parsedLimit = Integer.parseInt(limitArg); }
                    catch (NumberFormatException ex) { throw new IllegalArgumentException("limit() must be numeric"); }
                    if (!hopsSeen) preHopLimit = parsedLimit; else limit = parsedLimit;
                }
                case "count" -> { ensureArgCount(stepName, args, 0); countRequested = true; }
                case "sum"   -> { ensureArgCount(stepName, args, 0); sumRequested = true; }
                case "mean"  -> { ensureArgCount(stepName, args, 0); meanRequested = true; }
                case "groupCount" -> {
                    ensureArgCount(stepName, args, 0);
                    if (groupCountSeen) throw new IllegalArgumentException("Only a single groupCount() step is supported");
                    groupCountSeen = true;
                }
                case "project" -> {
                    if (args.isEmpty()) throw new IllegalArgumentException("project expects at least 1 argument");
                    if (!projectAliases.isEmpty()) throw new IllegalArgumentException("Only a single project(...) step is supported");
                    for (String arg : args) {
                        String a = unquote(arg);
                        if (a.isBlank()) throw new IllegalArgumentException("project aliases must be non-empty");
                        projectAliases.add(a);
                    }
                }
                case "outE" -> {
                    if (args.size() > 1) throw new IllegalArgumentException("outE expects 0 or 1 argument(s)");
                    hopsSeen = true;
                    hops.add(new HopStep(GremlinToken.OUT_E, args.isEmpty() ? List.of() : List.of(unquote(args.get(0)))));
                }
                case "inE" -> {
                    if (args.size() > 1) throw new IllegalArgumentException("inE expects 0 or 1 argument(s)");
                    hopsSeen = true;
                    hops.add(new HopStep(GremlinToken.IN_E, args.isEmpty() ? List.of() : List.of(unquote(args.get(0)))));
                }
                case "bothE" -> {
                    if (args.size() > 1) throw new IllegalArgumentException("bothE expects 0 or 1 argument(s)");
                    hopsSeen = true;
                    hops.add(new HopStep(GremlinToken.BOTH_E, args.isEmpty() ? List.of() : List.of(unquote(args.get(0)))));
                }
                case "as" -> {
                    ensureArgCount(stepName, args, 1);
                    String aliasName = unquote(args.get(0));
                    if (aliasName.isBlank()) throw new IllegalArgumentException("as() label must be non-empty");
                    asAliases.add(new AsAlias(aliasName, hops.size()));
                }
                case "select" -> {
                    if (args.isEmpty()) throw new IllegalArgumentException("select expects at least 1 argument");
                    if (selectSeen) throw new IllegalArgumentException("Only a single select(...) step is supported");
                    selectSeen = true;
                    for (String arg : args) selectAliases.add(unquote(arg));
                }
                case "where" -> {
                    if (whereClause != null) throw new IllegalArgumentException("Only a single where() step is supported");
                    whereClause = parseWhere(rawStepArgs);
                    whereByPending = whereClause.kind() == WhereKind.NEQ_ALIAS;
                }
                case "dedup"    -> { ensureArgCount(stepName, args, 0); dedupRequested = true; }
                case "identity" -> ensureArgCount(stepName, args, 0);
                case "path"     -> { ensureArgCount(stepName, args, 0); pathSeen = true; }
                case "by" -> {
                    if (args.isEmpty()) throw new IllegalArgumentException("by(...) expects at least 1 argument");
                    if (whereByPending) {
                        ensureArgCount(stepName, args, 1);
                        whereByProperty = unquote(args.get(0));
                        whereByPending = false;
                    } else if (selectSeen) {
                        if (selectByExpressions.size() >= selectAliases.size())
                            throw new IllegalArgumentException("select(...) has more by(...) modulators than selected aliases");
                        selectByExpressions.add(rawStepArgs);
                    } else if (orderSeen && orderByProperty == null && !orderByCountDesc) {
                        List<String> byArgsTrimmed = args.stream().map(String::trim).toList();
                        if (byArgsTrimmed.size() == 2 && ("values".equals(byArgsTrimmed.get(0)) || "__.values()".equals(byArgsTrimmed.get(0)))) {
                            String dirToken = byArgsTrimmed.get(1).toLowerCase();
                            orderByCountDesc = true;
                            orderDirection = (GremlinToken.ORDER_DESC.equals(dirToken) || GremlinToken.ORDER_DESC_FULL.equals(dirToken)) ? "DESC" : "ASC";
                        } else if (byArgsTrimmed.size() == 2 && !byArgsTrimmed.get(0).contains("(")) {
                            orderByProperty = unquote(byArgsTrimmed.get(0));
                            String dirToken = byArgsTrimmed.get(1).toLowerCase();
                            orderDirection = (dirToken.contains(GremlinToken.ORDER_DESC) || GremlinToken.ORDER_DESC_FULL.equals(dirToken)) ? "DESC" : "ASC";
                        } else if (rawStepArgs.contains("select(") && rawStepArgs.contains("Order.")) {
                            int selectStart = rawStepArgs.indexOf("select(") + 7;
                            int selectEnd = rawStepArgs.indexOf(")", selectStart);
                            orderByProperty = unquote(rawStepArgs.substring(selectStart, selectEnd));
                            orderDirection = rawStepArgs.contains("Order.desc") ? "DESC" : "ASC";
                        } else {
                            orderByProperty = unquote(rawStepArgs);
                            orderDirection = "ASC";
                        }
                    } else if (groupCountSeen && groupCountProperty == null && groupCountKeySpec == null) {
                        GroupCountKeySpec parsedKey = tryParseGroupCountSelectKey(rawStepArgs);
                        if (parsedKey != null) {
                            groupCountKeySpec = parsedKey;
                        } else {
                            ensureArgCount(stepName, args, 1);
                            groupCountProperty = unquote(args.get(0));
                            if (groupCountProperty.isBlank())
                                throw new IllegalArgumentException("by(...) property must be non-empty");
                        }
                    } else if (!projectAliases.isEmpty()) {
                        if (byExpressions.size() >= projectAliases.size())
                            throw new IllegalArgumentException("project(...) has more by(...) modulators than projected fields");
                        byExpressions.add(rawStepArgs);
                    } else if (pathSeen) {
                        ensureArgCount(stepName, args, 1);
                        pathByProperties.add(unquote(args.get(0)));
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

        if (pathSeen && pathByProperties.size() == 1 && valuesProperty == null)
            valuesProperty = pathByProperties.get(0);

        List<ProjectionField> projections = new ArrayList<>();
        for (int i = 0; i < projectAliases.size(); i++)
            projections.add(parseProjection(projectAliases.get(i), byExpressions.get(i)));

        List<SelectField> selectFields = new ArrayList<>();
        String sharedSelectBy = (selectAliases.size() > 1 && selectByExpressions.size() == 1)
                ? unquote(selectByExpressions.get(0)) : null;
        for (int i = 0; i < selectAliases.size(); i++) {
            String property = sharedSelectBy != null ? sharedSelectBy
                    : (i < selectByExpressions.size() ? unquote(selectByExpressions.get(i)) : null);
            selectFields.add(new SelectField(selectAliases.get(i), property));
        }

        if ((sumRequested || meanRequested) && valuesProperty == null) {
            if (selectFields.size() == 1) {
                SelectField sf = selectFields.get(0);
                if (sf.property() != null && !sf.property().isBlank()) {
                    valuesProperty = sf.property();
                } else {
                    for (ProjectionField pf : projections) {
                        if (pf.alias().equals(sf.alias()) && pf.kind() == ProjectionKind.EDGE_PROPERTY) {
                            valuesProperty = pf.property();
                            break;
                        }
                    }
                }
            }
            if (valuesProperty == null)
                throw new IllegalArgumentException(
                        (meanRequested ? "mean()" : "sum()") + " requires values('property') before aggregation");
        }

        return new ParsedTraversal(
                label, filters, valuesProperty, valueMapRequested,
                valueMapKeys, limit, preHopLimit, hops,
                countRequested, sumRequested, meanRequested,
                projections, groupCountProperty,
                orderByProperty, orderDirection,
                asAliases, selectFields, whereClause,
                dedupRequested, pathSeen, pathByProperties,
                whereByProperty, simplePathRequested,
                groupCountKeySpec, orderByCountDesc);
    }

    // ── Where parsing ─────────────────────────────────────────────────────────

    public WhereClause parseWhere(String rawWhere) {
        String expression = rawWhere == null ? "" : rawWhere.trim();

        if (expression.startsWith("and(") && expression.endsWith(")")) {
            String inner = expression.substring(4, expression.length() - 1).trim();
            List<String> argsList = splitTopLevelPredicateArgs(inner);
            if (argsList.size() < 2) throw new IllegalArgumentException("where(and(...)) requires at least two predicates");
            List<WhereClause> clauses = argsList.stream().map(this::parseWhere).toList();
            return new WhereClause(WhereKind.AND, null, null, List.of(), clauses);
        }
        if (expression.startsWith("or(") && expression.endsWith(")")) {
            String inner = expression.substring(3, expression.length() - 1).trim();
            List<String> argsList = splitTopLevelPredicateArgs(inner);
            if (argsList.size() < 2) throw new IllegalArgumentException("where(or(...)) requires at least two predicates");
            List<WhereClause> clauses = argsList.stream().map(this::parseWhere).toList();
            return new WhereClause(WhereKind.OR, null, null, List.of(), clauses);
        }
        if (expression.startsWith("not(") && expression.endsWith(")")) {
            String inner = expression.substring(4, expression.length() - 1).trim();
            if (inner.isBlank()) throw new IllegalArgumentException("where(not(...)) requires one predicate");
            return new WhereClause(WhereKind.NOT, null, null, List.of(), List.of(parseWhere(inner)));
        }

        Matcher neqMatcher = WHERE_NEQ_PATTERN.matcher(expression);
        if (neqMatcher.matches())
            return new WhereClause(WhereKind.NEQ_ALIAS, neqMatcher.group(1), neqMatcher.group(2), List.of());

        Matcher eqAliasMatcher = WHERE_EQ_ALIAS_PATTERN.matcher(expression);
        if (eqAliasMatcher.matches())
            return new WhereClause(WhereKind.EQ_ALIAS, eqAliasMatcher.group(1), null, List.of());

        Matcher gtMatcher = WHERE_SELECT_IS_GT_PATTERN.matcher(expression);
        if (gtMatcher.matches()) {
            boolean isGte = expression.contains(".is(gte(");
            return new WhereClause(isGte ? WhereKind.PROJECT_GTE : WhereKind.PROJECT_GT,
                    gtMatcher.group(1), gtMatcher.group(2).trim(), List.of());
        }

        Matcher edgeExistsMatcher = WHERE_EDGE_EXISTS_PATTERN.matcher(expression);
        if (edgeExistsMatcher.matches())
            return new WhereClause(WhereKind.EDGE_EXISTS, edgeExistsMatcher.group(1),
                    edgeExistsMatcher.group(2), parseHasChain(edgeExistsMatcher.group(3)));

        Matcher neighborMatcher = WHERE_OUT_NEIGHBOR_HAS_PATTERN.matcher(expression);
        if (neighborMatcher.matches()) {
            WhereKind kind = "out".equals(neighborMatcher.group(1)) ? WhereKind.OUT_NEIGHBOR_HAS : WhereKind.IN_NEIGHBOR_HAS;
            return new WhereClause(kind, neighborMatcher.group(1), neighborMatcher.group(2),
                    parseHasChain(neighborMatcher.group(3)));
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
            return new WhereClause(WhereKind.EDGE_EXISTS, direction, edgeLabel,
                    List.of(new HasFilter("count", rightValue, operator)));
        }

        throw new IllegalArgumentException("Unsupported where() predicate: " + expression);
    }

    // ── Projection parsing ────────────────────────────────────────────────────

    public ProjectionField parseProjection(String alias, String byExpression) {
        String expression = byExpression == null ? "" : byExpression.trim();

        Matcher degreeMatcher = EDGE_DEGREE_PATTERN.matcher(expression);
        if (degreeMatcher.matches()) {
            String direction = degreeMatcher.group(1).equals("outE") ? "out" : "in";
            String edgeLabel = degreeMatcher.group(2);
            List<HasFilter> degreeFilters = parseHasChain(degreeMatcher.group(3));
            StringBuilder encoded = new StringBuilder(direction + ":" + edgeLabel);
            for (HasFilter f : degreeFilters) encoded.append(":").append(f.property()).append("=").append(f.value());
            return new ProjectionField(alias, ProjectionKind.EDGE_DEGREE, encoded.toString());
        }

        Matcher vertexCountMatcher = VERTEX_COUNT_PATTERN.matcher(expression);
        if (vertexCountMatcher.matches()) {
            String direction = vertexCountMatcher.group(1);
            String edgeLabel = vertexCountMatcher.group(2);
            List<HasFilter> vcFilters = parseHasChain(vertexCountMatcher.group(3));
            StringBuilder encoded = new StringBuilder(direction + ":" + edgeLabel);
            for (HasFilter f : vcFilters) encoded.append(":").append(f.property()).append("=").append(f.value());
            ProjectionKind kind = "out".equals(direction) ? ProjectionKind.OUT_VERTEX_COUNT : ProjectionKind.IN_VERTEX_COUNT;
            return new ProjectionField(alias, kind, encoded.toString());
        }

        Matcher neighborValuesMatcher = NEIGHBOR_VALUES_FOLD_PATTERN.matcher(expression);
        if (neighborValuesMatcher.matches()) {
            String direction = neighborValuesMatcher.group(1);
            String edgeLabel = splitArgs(neighborValuesMatcher.group(2)).stream().map(this::unquote).toList().get(0);
            String prop = neighborValuesMatcher.group(3);
            ProjectionKind kind = "out".equals(direction) ? ProjectionKind.OUT_NEIGHBOR_PROPERTY : ProjectionKind.IN_NEIGHBOR_PROPERTY;
            return new ProjectionField(alias, kind, direction + ":" + edgeLabel + ":" + prop);
        }

        ChooseProjectionSpec chooseSpec = parseChooseProjection(expression);
        if (chooseSpec != null) return new ProjectionField(alias, ProjectionKind.CHOOSE_VALUES_IS_CONSTANT, expression);

        Matcher endpointMatcher = ENDPOINT_VALUES_PATTERN.matcher(expression);
        if (endpointMatcher.matches()) {
            String endpoint = endpointMatcher.group(1);
            String property = unquote(endpointMatcher.group(2));
            if (property.isBlank())
                throw new IllegalArgumentException("by(outV().values(...)) and by(inV().values(...)) require a property name");
            ProjectionKind kind = "outV".equals(endpoint) ? ProjectionKind.OUT_VERTEX_PROPERTY : ProjectionKind.IN_VERTEX_PROPERTY;
            return new ProjectionField(alias, kind, property);
        }

        if (GremlinToken.IDENTITY.equals(expression) || GremlinToken.IDENTITY_PREFIXED.equals(expression))
            return new ProjectionField(alias, ProjectionKind.IDENTITY, GremlinToken.PROP_ID);

        if (expression.contains("("))
            throw new IllegalArgumentException("Unsupported by() projection expression: " + expression);

        String property = unquote(expression);
        if (property.isBlank()) throw new IllegalArgumentException("by(...) property must be non-empty");
        return new ProjectionField(alias, ProjectionKind.EDGE_PROPERTY, property);
    }

    // ── Has-chain / has-argument parsing ─────────────────────────────────────

    public List<HasFilter> parseHasChain(String hasChain) {
        List<HasFilter> result = new ArrayList<>();
        if (hasChain == null || hasChain.isBlank()) return result;
        Matcher m = HAS_CHAIN_ITEM.matcher(hasChain);
        while (m.find()) result.add(new HasFilter(m.group(1), m.group(2)));
        return result;
    }

    public HasFilter parseHasArgument(String property, String rawValue) {
        String trimmed = rawValue.trim();
        Matcher m = HAS_PREDICATE_PATTERN.matcher(trimmed);
        if (m.matches()) {
            String predicate = m.group(1);
            String innerVal = unquote(m.group(2).trim());
            String operator = switch (predicate) {
                case "gt"  -> ">";
                case "gte" -> ">=";
                case "lt"  -> "<";
                case "lte" -> "<=";
                case "neq" -> "!=";
                default    -> "=";
            };
            return new HasFilter(property, innerVal, operator);
        }
        return new HasFilter(property, unquote(trimmed));
    }

    // ── Repeat hop parsing ────────────────────────────────────────────────────

    public RepeatHopResult parseRepeatHop(String repeatBodyRaw) {
        String text = repeatBodyRaw == null ? "" : repeatBodyRaw.trim();
        boolean hasSimplePath = text.endsWith(".simplePath()");
        if (hasSimplePath) text = text.substring(0, text.length() - ".simplePath()".length()).trim();
        if (!text.endsWith(")"))
            throw new IllegalArgumentException("Only repeat(out(...)), repeat(in(...)), or repeat(both(...)) is supported");

        String direction;
        int openParenIndex;
        if (text.startsWith("out("))       { direction = GremlinToken.OUT;  openParenIndex = 3; }
        else if (text.startsWith("in("))   { direction = GremlinToken.IN;   openParenIndex = 2; }
        else if (text.startsWith("both(")) { direction = GremlinToken.BOTH; openParenIndex = 4; }
        else throw new IllegalArgumentException("Only repeat(out(...)), repeat(in(...)), or repeat(both(...)) is supported");

        String inner = text.substring(openParenIndex + 1, text.length() - 1).trim();
        if (inner.isEmpty()) return new RepeatHopResult(new HopStep(direction, List.of()), hasSimplePath);
        List<String> labels = splitArgs(inner).stream().map(this::unquote).toList();
        return new RepeatHopResult(new HopStep(direction, labels), hasSimplePath);
    }

    // ── Choose projection spec ────────────────────────────────────────────────

    public ChooseProjectionSpec parseChooseProjection(String expression) {
        if (expression == null || expression.isBlank()) return null;
        Matcher matcher = CHOOSE_VALUES_IS_CONSTANT_PATTERN.matcher(expression.trim());
        return matcher.matches()
                ? new ChooseProjectionSpec(
                        unquote(matcher.group(1)),
                        matcher.group(2),
                        matcher.group(3).trim(),
                        matcher.group(4).trim(),
                        matcher.group(5).trim())
                : null;
    }

    // ── GroupCount key spec ───────────────────────────────────────────────────

    public GroupCountKeySpec tryParseGroupCountSelectKey(String rawExpr) {
        if (rawExpr == null || rawExpr.isBlank()) return null;
        Matcher m = GROUP_COUNT_SELECT_BY_PATTERN.matcher(rawExpr.trim());
        return m.matches() ? new GroupCountKeySpec(m.group(1), m.group(2)) : null;
    }

    // ── Normalization helpers ─────────────────────────────────────────────────

    /**
     * Normalises an {@code outV()} or {@code inV()} step that follows an {@code outE()} or
     * {@code inE()} step by collapsing the pair into a single direction hop.
     *
     * @return {@code true} if the step should be appended as-is; {@code false} if it was merged
     */
    public boolean normalizeEndpointHop(String endpointStep, List<HopStep> hops) {
        if (hops.isEmpty()) return true;
        if (hops.size() > 1) {
            HopStep beforePrevious = hops.get(hops.size() - 2);
            if (GremlinToken.OUT_V.equals(beforePrevious.direction()) || GremlinToken.IN_V.equals(beforePrevious.direction())) return true;
        }
        HopStep previous = hops.get(hops.size() - 1);
        if (!GremlinToken.OUT_E.equals(previous.direction()) && !GremlinToken.IN_E.equals(previous.direction())) return true;
        hops.remove(hops.size() - 1);
        String normalized;
        if (GremlinToken.OUT_E.equals(previous.direction())) {
            normalized = GremlinToken.IN_V.equals(endpointStep) ? GremlinToken.OUT : GremlinToken.IN;
        } else {
            normalized = GremlinToken.IN_V.equals(endpointStep) ? GremlinToken.IN : GremlinToken.OUT;
        }
        hops.add(new HopStep(normalized, previous.labels()));
        return false;
    }

    // ── String utilities ──────────────────────────────────────────────────────

    public String unquote(String text) {
        String trimmed = text.trim();
        if ((trimmed.startsWith("\"") && trimmed.endsWith("\""))
                || (trimmed.startsWith("'") && trimmed.endsWith("'"))) {
            return trimmed.substring(1, trimmed.length() - 1);
        }
        return trimmed;
    }

    public List<String> splitArgs(String argsText) {
        List<String> result = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inSingleQuote = false;
        boolean inDoubleQuote = false;
        for (int i = 0; i < argsText.length(); i++) {
            char c = argsText.charAt(i);
            if (c == '\'' && !inDoubleQuote) { inSingleQuote = !inSingleQuote; current.append(c); }
            else if (c == '"' && !inSingleQuote) { inDoubleQuote = !inDoubleQuote; current.append(c); }
            else if (c == ',' && !inSingleQuote && !inDoubleQuote) {
                result.add(current.toString().trim()); current.setLength(0);
            } else { current.append(c); }
        }
        if (!argsText.isBlank()) result.add(current.toString().trim());
        return result;
    }

    public List<String> splitTopLevelPredicateArgs(String text) {
        List<String> result = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inSingleQuote = false;
        boolean inDoubleQuote = false;
        int depth = 0;
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c == '\'' && !inDoubleQuote) { inSingleQuote = !inSingleQuote; current.append(c); continue; }
            if (c == '"'  && !inSingleQuote) { inDoubleQuote = !inDoubleQuote; current.append(c); continue; }
            if (!inSingleQuote && !inDoubleQuote) {
                if (c == '(') depth++;
                else if (c == ')') depth--;
                else if (c == ',' && depth == 0) { result.add(current.toString().trim()); current.setLength(0); continue; }
            }
            current.append(c);
        }
        if (!current.isEmpty()) result.add(current.toString().trim());
        return result;
    }

    private static void ensureArgCount(String step, List<String> args, int expected) {
        if (args.size() != expected)
            throw new IllegalArgumentException(step + " expects " + expected + " argument(s)");
    }
}

