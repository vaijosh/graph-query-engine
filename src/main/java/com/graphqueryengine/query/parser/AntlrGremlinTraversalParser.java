package com.graphqueryengine.query.parser;

import com.graphqueryengine.query.parser.model.GremlinParseResult;
import com.graphqueryengine.query.parser.model.GremlinStep;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

/**
 * ANTLR parser mode backed by Gremlin.g4 grammar that maps parse-tree nodes
 * into the shared {@link GremlinParseResult} model.
 */
public class AntlrGremlinTraversalParser implements GremlinTraversalParser {
    private static final String LEXER_CLASS = "com.graphqueryengine.query.gremlin.GremlinLexer";
    private static final String PARSER_CLASS = "com.graphqueryengine.query.gremlin.GremlinParser";

    @Override
    public GremlinParseResult parse(String gremlin) {
        if (gremlin == null || gremlin.isBlank()) {
            throw new IllegalArgumentException("Gremlin query is required");
        }

        try {
            Class<?> lexerType = Class.forName(LEXER_CLASS);
            Lexer lexer = (Lexer) lexerType
                    .getConstructor(org.antlr.v4.runtime.CharStream.class)
                    .newInstance(CharStreams.fromString(gremlin.trim()));

            Class<?> parserType = Class.forName(PARSER_CLASS);
            Parser parser = (Parser) parserType
                    .getConstructor(org.antlr.v4.runtime.TokenStream.class)
                    .newInstance(new CommonTokenStream(lexer));

            ThrowingErrorListener errorListener = new ThrowingErrorListener();
            lexer.removeErrorListeners();
            parser.removeErrorListeners();
            lexer.addErrorListener(errorListener);
            parser.addErrorListener(errorListener);

            Object queryCtx = parserType.getMethod("query").invoke(parser);
            Object traversalCtx = queryCtx.getClass().getMethod("traversal").invoke(queryCtx);
            @SuppressWarnings("unchecked")
            List<Object> stepCtxs = (List<Object>) traversalCtx.getClass().getMethod("step").invoke(traversalCtx);
            List<GremlinStep> allSteps = new ArrayList<>();
            for (Object stepCtx : stepCtxs) {
                TerminalNode ident = (TerminalNode) stepCtx.getClass().getMethod("IDENT").invoke(stepCtx);
                String stepName = ident.getText();

                List<String> args = new ArrayList<>();
                Object argumentsCtx = stepCtx.getClass().getMethod("arguments").invoke(stepCtx);
                if (argumentsCtx != null) {
                    @SuppressWarnings("unchecked")
                    List<ParseTree> argumentCtxs = (List<ParseTree>) argumentsCtx.getClass().getMethod("argument").invoke(argumentsCtx);
                    for (ParseTree argumentCtx : argumentCtxs) {
                        args.add(argumentCtx.getText());
                    }
                }
                allSteps.add(new GremlinStep(stepName, List.copyOf(args), String.join(",", args)));
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
        } catch (IllegalArgumentException ex) {
            throw ex;
        } catch (ClassNotFoundException ex) {
            throw new IllegalStateException(
                    "ANTLR Gremlin parser not available. " +
                    "To enable ANTLR parsing, add org.antlr:antlr4-runtime:4.9.3 to your dependencies.",
                    ex
            );
        } catch (InvocationTargetException ex) {
            Throwable cause = ex.getCause();
            if (cause instanceof IllegalArgumentException iae) {
                throw iae;
            }
            throw new IllegalStateException("Failed to initialize ANTLR Gremlin parser", cause == null ? ex : cause);
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to initialize ANTLR Gremlin parser", ex);
        }
    }

    private String unquote(String text) {
        String trimmed = text.trim();
        if ((trimmed.startsWith("\"") && trimmed.endsWith("\"")) ||
                (trimmed.startsWith("'") && trimmed.endsWith("'"))) {
            return trimmed.substring(1, trimmed.length() - 1);
        }
        return trimmed;
    }

    private static final class ThrowingErrorListener extends BaseErrorListener {
        @Override
        public void syntaxError(Recognizer<?, ?> recognizer,
                                Object offendingSymbol,
                                int line,
                                int charPositionInLine,
                                String msg,
                                RecognitionException e) {
            throw new IllegalArgumentException(
                    "ANTLR Gremlin parse failed at line " + line + ", column " + charPositionInLine + ": " + msg,
                    e
            );
        }
    }
}

