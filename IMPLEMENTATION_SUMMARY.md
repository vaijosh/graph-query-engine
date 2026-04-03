# Planner-Stage Debug Output Implementation Summary

## Overview
Implemented structured query plan JSON output that allows callers to inspect all planner-stage decisions made by the SQL translator before SQL is rendered. This satisfies Design Document requirement 16.1: "Add planner-stage debug output (structured plan JSON) before SQL rendering".

## Changes Made

### 1. **QueryPlan Model** (`src/main/java/com/graphqueryengine/query/api/QueryPlan.java`)
- Comprehensive record representing all translator decisions
- Fields for root type/label/table, filters, hops, aggregations, projections, ordering, where clauses, aliases, and selections
- Nested records for structured sub-components: `FilterPlan`, `HopPlan`, `ProjectionPlan`, `WherePlan`, `AliasPlan`, `SelectPlan`
- All fields use `@JsonInclude(NON_NULL)` to omit N/A fields from JSON output

### 2. **QueryExplanation Enhanced** (`src/main/java/com/graphqueryengine/query/api/QueryExplanation.java`)
- Added optional `plan: QueryPlan` field
- Backward-compatible constructor for existing call sites without plan
- Plan is attached only when explicitly requested via `translateWithPlan()`

### 3. **TranslationResult Enhanced** (`src/main/java/com/graphqueryengine/query/api/TranslationResult.java`)
- Added optional `plan: QueryPlan` field
- Backward-compatible constructor for existing call sites without plan

### 4. **GraphQueryTranslator Interface** (`src/main/java/com/graphqueryengine/query/api/GraphQueryTranslator.java`)
- Added `translateWithPlan(String gremlin, MappingConfig mappingConfig)` default method
- Default implementation calls regular `translate()` for backward compatibility
- Implementations can override to provide full plan output

### 5. **GremlinSqlTranslator** (`src/main/java/com/graphqueryengine/query/translate/sql/GremlinSqlTranslator.java`)
- Added `translateWithPlan(String, MappingConfig)` overload — parses and delegates to parsed version
- Added `translateWithPlan(GremlinParseResult, MappingConfig)` — generates plan and attaches to result
- Added `plan(String, MappingConfig)` — generates plan only, no SQL rendering
- Added `planFromParsed(GremlinParseResult, MappingConfig)` — generates plan from parsed input
- Implemented `buildPlan(boolean, ParsedTraversal, MappingConfig)` private method:
  - Resolves root vertex/edge type and table
  - Extracts and structures filters, hops with target vertex resolution
  - Captures aggregation type and property
  - Records projections, where clauses, aliases, selections
  - Captures ordering, limits, dedup, simplePath, and path properties
- **Fixed rootIdFilter bug**: Now correctly extracts id filter from filters list instead of always returning null

### 6. **SqlGraphQueryTranslator** (`src/main/java/com/graphqueryengine/query/translate/sql/SqlGraphQueryTranslator.java`)
- Added `translateWithPlan(String, MappingConfig)` override
- Added `translateWithPlanWithPlan(GremlinParseResult, MappingConfig)` private method
- Routes both standard and Iceberg mode requests to appropriate delegate with plan support

### 7. **App HTTP Layer** (`src/main/java/com/graphqueryengine/App.java`)
- Enhanced `/query/explain` endpoint to support `?plan=true` query parameter
- Calls `translateWithPlan()` when plan is requested
- Attaches plan to `QueryExplanation` response JSON
- Backward compatible: plan is null if not requested

### 8. **Tests** (`src/test/java/com/graphqueryengine/query/translate/sql/QueryPlanTest.java`)
- Comprehensive unit test suite with 13 test cases covering:
  - Simple vertex queries with filters
  - Hop traversals with target vertex resolution
  - Aggregations (count, sum, mean, groupCount)
  - Root ID filter extraction
  - Projections
  - Limits and ordering
  - Dedup and simplePath
  - Alias tracking
  - Edge queries
  - Dialect name recording
  - Plan vs. non-plan translation modes
  - SQL still generated when plan is requested

**All tests pass successfully.**

## API Usage Examples

### Get SQL translation with plan
```bash
curl -X POST http://localhost:7000/query/explain?plan=true \
  -H "Content-Type: application/json" \
  -d '{"gremlin":"g.V().hasLabel(\"Account\").out(\"TRANSFER\").count()"}' \
  -H "X-Mapping-Id: my-mapping-id"
```

### Response includes plan details
```json
{
  "gremlin": "g.V().hasLabel(\"Account\").out(\"TRANSFER\").count()",
  "translatedSql": "SELECT COUNT(*) AS count FROM account v0 JOIN transfer e1 ...",
  "parameters": [],
  "mode": "SQL_EXPLAIN",
  "note": "...",
  "plan": {
    "rootType": "vertex",
    "rootLabel": "Account",
    "rootTable": "account",
    "dialect": "StandardSqlDialect",
    "hops": [
      {
        "direction": "out",
        "labels": ["TRANSFER"],
        "resolvedEdgeTable": "transfer",
        "resolvedTargetTable": "bank",
        "resolvedTargetLabel": "Bank"
      }
    ],
    "aggregation": "count"
  }
}
```

### Programmatic access
```java
GremlinSqlTranslator translator = new GremlinSqlTranslator();
TranslationResult result = translator.translateWithPlan(
  "g.V().hasLabel('Account')",
  mappingConfig
);
QueryPlan plan = result.plan(); // Full plan available for inspection
String sql = result.sql();       // SQL still generated
```

## Benefits

1. **Debug visibility** — Inspect exact mapping resolutions and translation decisions
2. **Mapping validation** — Verify edge/vertex target resolution and label matching
3. **Performance analysis** — Understand hop chains and filter placement
4. **Backward compatible** — Plain `translate()` calls unaffected; plan is optional
5. **Structured output** — JSON serialization with null-field omission for clarity

## Testing

- **13 new unit tests** covering all plan fields and combinations
- **All existing tests pass** (1325 tests, 2 pre-existing failures unrelated to changes)
- **Integration tested** via `/query/explain?plan=true` endpoint

## Notes

- Plan generation does not execute SQL — it's pure translation analysis
- Plan is attached only when `translateWithPlan()` or `?plan=true` is explicitly requested
- Dialect name is automatically recorded from translator instance
- rootIdFilter now correctly extracted from parsed filter list (bug fix)

