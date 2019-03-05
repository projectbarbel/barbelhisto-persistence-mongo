package com.projectbarbel.histo.persistence.mongo;

import java.io.File;
import java.util.function.Function;

import org.junit.jupiter.api.Test;
import org.projectbarbel.histo.BarbelHistoBuilder;
import org.projectbarbel.histo.BarbelHistoContext;
import org.projectbarbel.histo.suite.BTSuiteExecutor;
import org.projectbarbel.histo.suite.context.BTTestContextCQEngine;
import org.projectbarbel.histo.suite.extensions.DisableOnTravis;

import com.googlecode.cqengine.ConcurrentIndexedCollection;
import com.googlecode.cqengine.persistence.disk.DiskPersistence;
import com.mongodb.client.MongoDatabase;

@DisableOnTravis
public class PerformanceSuiteTest {

    private static SimpleMongoListenerClient client = SimpleMongoListenerClient.INSTANCE;

    @Test
    public void performanceSetupTest() throws InterruptedException {
        BTSuiteExecutor executor = new BTSuiteExecutor();
        executor.test(new BTTestContextPerformance());
        Thread.sleep(5000);
    }

    public static class BTTestContextPerformance extends BTTestContextCQEngine {

        @Override
        public Function<Class<?>, BarbelHistoBuilder> contextFunction() {
            return new Function<Class<?>, BarbelHistoBuilder>() {

                @SuppressWarnings("unchecked")
                @Override
                public BarbelHistoBuilder apply(Class<?> managedType) {
                    SimpleMongoUpdateListener updateListener = SimpleMongoUpdateListener.create(client.getMongoClient(),
                            "performanceDb", "testCol", managedType, BarbelHistoContext.getDefaultGson());
                    return BarbelHistoBuilder.barbel()
                            .withBackboneSupplier(() -> new ConcurrentIndexedCollection<>(DiskPersistence
                                    .onPrimaryKeyInFile(getAttribute(managedType), new File("test.dat"))))
                            .withAsynchronousEventListener(updateListener);
                }

            };

        }

        @Override
        public void clearResources() {
            super.clearResources();
            if (client != null) {
                MongoDatabase database = client.getMongoClient().getDatabase("performanceDb");
                if (database != null)
                    database.drop();
            }
        }

    }

}
