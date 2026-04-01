package com.graphqueryengine.gremlin.tinkerpop;

import io.cucumber.junit.Cucumber;
import io.cucumber.junit.CucumberOptions;
import org.junit.runner.RunWith;

/**
 * TinkerPop Gremlin Process-Standard compatibility suite.
 *
 * <p>Executes the official TinkerPop BDD feature files (bundled inside
 * {@code gremlin-test-3.7.4.jar}) against a live {@link org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph}
 * instance backed by the standard reference graphs (modern, classic, crew, grateful, sink).
 * Each {@code .feature} scenario maps through TinkerPop's own {@code StepDefinition}
 * class, which is injected with our {@link TinkerPopWorld} via Guice.
 *
 * <p><b>Steps validated (read-only traversals):</b>
 * <ul>
 *   <li>Filter: {@code has()}, {@code hasLabel()}, {@code where()}, {@code dedup()},
 *       {@code simplePath()}, {@code range()}, {@code is()}, {@code and()}, {@code or()}, etc.</li>
 *   <li>Map: {@code count()}, {@code values()}, {@code path()}, {@code select()},
 *       {@code project()}, {@code order()}, {@code fold()}, {@code unfold()},
 *       {@code valueMap()}, {@code elementMap()}, {@code math()}, string steps, etc.</li>
 *   <li>Branch: {@code repeat()}, {@code union()}, {@code choose()}, {@code optional()},
 *       {@code coalesce()}, etc.</li>
 *   <li>SideEffect: {@code group()}, {@code groupCount()}, {@code aggregate()}, etc.</li>
 * </ul>
 *
 * <p><b>Excluded (require graph mutation or graph-computer/OLAP):</b>
 * <ul>
 *   <li>{@code @GraphComputerOnly} — PageRank, ConnectedComponent, PeerPressure, ShortestPath</li>
 *   <li>{@code @GraphComputerVerificationRequired} / {@code @GraphComputerVerificationReferenceOnly}</li>
 *   <li>{@code @GraphComputerVerificationStarGraphExceeded}</li>
 *   <li>{@code @RemoteOnly}</li>
 *   <li>{@code @StepAddV}, {@code @StepAddE} — vertex/edge mutation</li>
 *   <li>{@code @StepMergeV}, {@code @StepMergeE} — merge mutation</li>
 *   <li>{@code @StepDrop} — element deletion</li>
 *   <li>{@code @StepRead}, {@code @StepWrite} — IO steps</li>
 *   <li>{@code @StepCall}, {@code @TinkerServiceRegistry} — service-registry step</li>
 * </ul>
 *
 * <p>cucumber-guice wires the object factory — configured in
 * {@code src/test/resources/cucumber.properties}.
 */
@RunWith(Cucumber.class)
@CucumberOptions(
        // Feature files are loaded from the gremlin-test jar on the classpath
        features = "classpath:org/apache/tinkerpop/gremlin/test/features",
        // Step glue: TinkerPop's StepDefinition + our InjectorSource
        glue = {
            "org.apache.tinkerpop.gremlin.features",   // TinkerPop StepDefinition
            "com.graphqueryengine.gremlin.tinkerpop"   // TinkerPopGuiceFactory (InjectorSource)
        },
        // Exclude graph-computer-only, mutating, and IO scenarios using exact feature tags
        tags = "not @GraphComputerOnly" +
               " and not @GraphComputerVerificationRequired" +
               " and not @GraphComputerVerificationReferenceOnly" +
               " and not @GraphComputerVerificationStarGraphExceeded" +
               " and not @RemoteOnly" +
               " and not @StepAddV" +
               " and not @StepAddE" +
               " and not @StepMergeV" +
               " and not @StepMergeE" +
               " and not @StepDrop" +
               " and not @StepRead" +
               " and not @StepWrite" +
               " and not @StepCall" +
               " and not @TinkerServiceRegistry",
        plugin = {
            "pretty",
            "summary",
            "html:target/cucumber-reports/gremlin-compatibility.html"
        }
)
public class GremlinProcessStandardSuiteTest {
    // JUnit 4 Cucumber runner — no body needed.
}
