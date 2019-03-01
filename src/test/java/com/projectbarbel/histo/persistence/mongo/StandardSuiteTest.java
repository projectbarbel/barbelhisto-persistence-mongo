package com.projectbarbel.histo.persistence.mongo;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.PrintWriter;
import java.util.function.Function;

import org.junit.jupiter.api.Test;
import org.junit.platform.engine.discovery.ClassNameFilter;
import org.junit.platform.engine.discovery.DiscoverySelectors;
import org.junit.platform.launcher.Launcher;
import org.junit.platform.launcher.LauncherDiscoveryRequest;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestIdentifier;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;
import org.junit.platform.launcher.listeners.SummaryGeneratingListener;
import org.junit.platform.launcher.listeners.TestExecutionSummary;
import org.projectbarbel.histo.BarbelHistoBuilder;
import org.projectbarbel.histo.BarbelHistoContext;
import org.projectbarbel.histo.BarbelHistoTestContext;
import org.slf4j.LoggerFactory;

import com.projectbarbel.histo.persistence.impl.mongo.FlapDoodleEmbeddedMongo;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;

public class StandardSuiteTest {
    
    SummaryGeneratingListener listener = new SummaryGeneratingListener();
    
    public void runall() {
        LauncherDiscoveryRequest request = LauncherDiscoveryRequestBuilder.request()
                .selectors(
                    DiscoverySelectors.selectPackage("org.projectbarbel.histo")
                )
                .filters(
                        ClassNameFilter.includeClassNamePatterns(".*SuiteTest")
                )
                .build();

            Launcher launcher = LauncherFactory.create();

            // Register a listener of your choice
            launcher.registerTestExecutionListeners(listener, new TestExecutionListener() {
                public void executionStarted(TestIdentifier testIdentifier) {
                    System.out.println(testIdentifier.getDisplayName());
                }
            });

            launcher.execute(request);
    }
    
    @Test
    public void standardTestSuite() {
            StandardSuiteTest launcher = new StandardSuiteTest();
            BarbelHistoTestContext.INSTANCE = new Function<Class<?>,BarbelHistoBuilder> () {

                @Override
                public BarbelHistoBuilder apply(Class<?> managedType) {
                    FlapDoodleEmbeddedMongo.instance();
                    LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
                    Logger rootLogger = loggerContext.getLogger("org.mongodb.driver");
                    rootLogger.setLevel(Level.OFF);
                    SimpleMongoListenerClient client = SimpleMongoListenerClient.INSTANCE;
                    client.getMongoClient().getDatabase("testDb").drop();
                    SimpleMongoUpdateListener updateListener = SimpleMongoUpdateListener.create(client.getMongoClient(), "testDb",
                            "testCol", managedType, BarbelHistoContext.getDefaultGson());
                    SimpleMongoLazyLoadingListener loadingListener = SimpleMongoLazyLoadingListener.create(client.getMongoClient(), "testDb",
                            "testCol", managedType, BarbelHistoContext.getDefaultGson());
                    MongoPessimisticLockingListener locking = MongoPessimisticLockingListener.create(client.getMongoClient(), "lockDb", "docLocks");
                    return BarbelHistoBuilder.barbel().withSynchronousEventListener(updateListener)
                            .withSynchronousEventListener(loadingListener).withSynchronousEventListener(locking);
                }
            };
            launcher.runall();
            TestExecutionSummary summary = launcher.listener.getSummary();
            summary.printTo(new PrintWriter(System.out));
            summary.printFailuresTo(new PrintWriter(System.out));
            assertEquals(0, summary.getFailures().size());
    }
}
