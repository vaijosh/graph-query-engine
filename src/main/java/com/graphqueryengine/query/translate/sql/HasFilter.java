package com.graphqueryengine.query.translate.sql;

import com.graphqueryengine.query.translate.sql.constant.SqlOperator;

/**
 * A single {@code has(property, value)} or {@code has(property, predicate(value))} filter.
 *
 * <p>The {@code operator} field holds the SQL comparison operator:
 * <ul>
 *   <li>{@code "="} — equality (default, from {@code has('prop','val')})</li>
 *   <li>{@code ">"} — from {@code gt(val)}</li>
 *   <li>{@code ">="} — from {@code gte(val)}</li>
 *   <li>{@code "<"} — from {@code lt(val)}</li>
 *   <li>{@code "<="} — from {@code lte(val)}</li>
 *   <li>{@code "!="} — from {@code neq(val)}</li>
 * </ul>
 *
 * <p>For equality/inequality filters the value is always bound as a {@link String} to preserve
 * compatibility with string-typed id columns.  For range operators ({@code >}, {@code >=},
 * {@code <}, {@code <=}) the value is converted to its natural Java numeric type via
 * {@link #typedValue()} so that strictly-typed databases (e.g. Trino/Iceberg) do not reject
 * comparisons of numeric columns against a VARCHAR parameter.
 */
public record HasFilter(String property, String value, String operator) {

    /** Convenience constructor for equality filters (the common case). */
    public HasFilter(String property, String value) {
        this(property, value, SqlOperator.EQ);
    }

    /**
     * Returns the value as the appropriate Java type for use as a JDBC parameter.
     *
     * <ul>
     *   <li>For <em>range</em> operators ({@code >}, {@code >=}, {@code <}, {@code <=}):
     *       unquoted integers are returned as {@link Long}, unquoted decimals as {@link Double}.</li>
     *   <li>For all other operators (equality, inequality, IS NULL): the raw string is returned
     *       so that string-typed id/name columns work correctly.</li>
     * </ul>
     */
    public Object typedValue() {
        if (value == null) return null;
        String v = value.trim();

        // Only coerce to numeric for range operators – equality stays as String to keep id
        // lookups working on string-typed columns.
        boolean isRangeOperator = SqlOperator.GT.equals(operator) || SqlOperator.GTE.equals(operator)
                || SqlOperator.LT.equals(operator) || SqlOperator.LTE.equals(operator);

        if (isRangeOperator) {
            // Quoted literals inside a range predicate are unusual but respect them as strings.
            if ((v.startsWith("'") && v.endsWith("'")) || (v.startsWith("\"") && v.endsWith("\""))) {
                return v.substring(1, v.length() - 1);
            }
            // Try integer first (Long covers both INT and BIGINT)
            try { return Long.parseLong(v); } catch (NumberFormatException ignored) {}
            // Then double
            try { return Double.parseDouble(v); } catch (NumberFormatException ignored) {}
        }

        // Strip surrounding quotes for equality/inequality; keep value as String.
        if ((v.startsWith("'") && v.endsWith("'")) || (v.startsWith("\"") && v.endsWith("\""))) {
            return v.substring(1, v.length() - 1);
        }
        return v;
    }
}

