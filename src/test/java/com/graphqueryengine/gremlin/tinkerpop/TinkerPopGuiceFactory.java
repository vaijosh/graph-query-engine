package com.graphqueryengine.gremlin.tinkerpop;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Stage;
import io.cucumber.guice.CucumberModules;
import io.cucumber.guice.GuiceFactory;
import io.cucumber.guice.InjectorSource;
import org.apache.tinkerpop.gremlin.features.World;

/**
 * Guice {@link InjectorSource} for the TinkerPop Gremlin compatibility suite.
 *
 * <p>cucumber-guice calls {@link #getInjector()} once per Cucumber run to build the
 * application-level Guice injector.  All Cucumber step-definition classes annotated with
 * {@code @ScenarioScoped} or {@code @Singleton} will be resolved from this injector.
 *
 * <p>The binding {@code World → TinkerPopWorld} makes our {@link TinkerPopWorld} available
 * to TinkerPop's {@code StepDefinition} class, which declares a constructor-injected
 * {@code World} dependency.
 *
 * <p>Registering this class as the factory:
 * <pre>
 *   cucumber.properties:
 *     cucumber.objectfactory=io.cucumber.guice.GuiceFactory
 *   (or) META-INF/services/io.cucumber.core.backend.ObjectFactory → io.cucumber.guice.GuiceFactory
 * </pre>
 * is handled via {@code src/test/resources/cucumber.properties}.
 */
public class TinkerPopGuiceFactory implements InjectorSource {

    @Override
    public Injector getInjector() {
        return Guice.createInjector(Stage.PRODUCTION,
                CucumberModules.createScenarioModule(),
                new TinkerPopModule());
    }

    /**
     * Guice module that binds the TinkerPop {@link World} interface to our
     * {@link TinkerPopWorld} implementation as a singleton (one graph set per JVM run).
     */
    static class TinkerPopModule extends AbstractModule {
        @Override
        protected void configure() {
            bind(World.class).to(TinkerPopWorld.class).in(com.google.inject.Singleton.class);
        }
    }
}

