package com.projectbarbel.histo.persistence.mongo;

import java.util.function.Function;

import org.junit.jupiter.api.Test;
import org.projectbarbel.histo.BarbelHistoBuilder;
import org.projectbarbel.histo.BarbelHistoContext;
import org.projectbarbel.histo.suite.BTSuiteExecutor;
import org.projectbarbel.histo.suite.context.BTTestContextPersistenceListener;
import org.slf4j.LoggerFactory;

import com.mongodb.client.MongoDatabase;
import com.projectbarbel.histo.persistence.impl.mongo.FlapDoodleEmbeddedMongo;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;

public class StandardSuiteTest {

    @Test
    public void standardTestSuite_singletonContext() {
        BTSuiteExecutor executor = new BTSuiteExecutor();
        executor.test(new BTTestContextPersistenceListener() {

            private SimpleMongoListenerClient client;

            @Override
            public Function<Class<?>, BarbelHistoBuilder> contextFunction() {
                return new Function<Class<?>, BarbelHistoBuilder>() {

                    @Override
                    public BarbelHistoBuilder apply(Class<?> managedType) {
                        FlapDoodleEmbeddedMongo.create();
                        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
                        Logger rootLogger = loggerContext.getLogger("org.mongodb.driver");
                        rootLogger.setLevel(Level.OFF);
                        client = SimpleMongoListenerClient.INSTANCE;
                        SimpleMongoUpdateListener updateListener = SimpleMongoUpdateListener.create(
                                client.getMongoClient(), "testSuiteDb", "testCol", managedType,
                                BarbelHistoContext.getDefaultGson());
                        SimpleMongoLazyLoadingListener loadingListener = SimpleMongoLazyLoadingListener.create(
                                client.getMongoClient(), "testSuiteDb", "testCol", managedType,
                                BarbelHistoContext.getDefaultGson(), true, true);
                        MongoPessimisticLockingListener locking = MongoPessimisticLockingListener
                                .create(client.getMongoClient(), "tsLockDb", "docLocks");
                        return BarbelHistoBuilder.barbel().withSynchronousEventListener(updateListener)
                                .withSynchronousEventListener(loadingListener).withSynchronousEventListener(locking);
                    }
                };
            }

            @Override
            public void clearResources() {
                if (client != null) {
                    MongoDatabase database = client.getMongoClient().getDatabase("testSuiteDb");
                    if (database != null)
                        database.drop();
                    database = client.getMongoClient().getDatabase("tsLockDb");
                    if (database != null)
                        database.drop();                    
                }
            }
        });
    }
}
