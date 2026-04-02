package com.graphqueryengine.query.parser;

import com.graphqueryengine.query.parser.model.GremlinParseResult;
import com.graphqueryengine.query.parser.model.GremlinStep;

import java.util.ArrayList;
import java.util.List;

/**
 * Legacy parser mode tokenizes directly from text and emits the shared parse model.
 */
public class LegacyGremlinTraversalParser implements GremlinTraversalParser {
    @Override
    public GremlinParseResult parse(String gremlin) {
        if (gremlin == null || gremlin.isBlank()) {
            throw new IllegalArgumentException("Gremlin query is required");
        }

        String query = gremlin.trim();
        if (!query.startsWith("g.")) {
            throw new IllegalArgumentException("Gremlin query must start with g.");
        }

        List<GremlinStep> allSteps = new ArrayList<>();
        int index = 1; // points at the first '.' after 'g'
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
            String rawArgs = parsedArgs.args().trim();
            List<String> args = splitArgs(parsedArgs.args());
            allSteps.add(new GremlinStep(stepName, args, rawArgs));
            index = parsedArgs.nextIndex();
        }

        if (allSteps.isEmpty()) {
            throw new IllegalArgumentException("Only g.V(...) and g.E(...) traversals are supported");
        }

        GremlinStep root = allSteps.get(0);
        boolean vertexQuery;
        if ("V".equals(root.name())) {
            vertexQuery = true;
        } else if ("E".equals(root.name())) {
            vertexQuery = false;
        } else {
            throw new IllegalArgumentException("Only g.V(...) and g.E(...) traversals are supported");
        }
        if (root.args().size() > 1) {
            throw new IllegalArgumentException("g." + root.name() + "() expects 0 or 1 argument(s)");
        }

        String rootIdFilter = root.args().isEmpty() || root.args().get(0).isBlank()
                ? null
                : unquote(root.args().get(0));

        List<GremlinStep> steps = allSteps.size() == 1
                ? List.of()
                : List.copyOf(allSteps.subList(1, allSteps.size()));
        return new GremlinParseResult(vertexQuery, rootIdFilter, steps);
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
}

