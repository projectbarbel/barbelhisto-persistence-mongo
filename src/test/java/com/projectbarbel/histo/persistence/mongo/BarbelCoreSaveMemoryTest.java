package com.projectbarbel.histo.persistence.mongo;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Test;
import org.projectbarbel.histo.BarbelHisto;
import org.projectbarbel.histo.BarbelHistoBuilder;
import org.projectbarbel.histo.BarbelHistoContext;
import org.projectbarbel.histo.BarbelHistoCore;
import org.projectbarbel.histo.model.DefaultPojo;
import org.slf4j.LoggerFactory;

import com.projectbarbel.histo.persistence.impl.mongo.BarbelTestHelper;
import com.projectbarbel.histo.persistence.impl.mongo.FlapDoodleEmbeddedMongo;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import io.github.benas.randombeans.api.EnhancedRandom;

public class BarbelCoreSaveMemoryTest {

    static ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1); // no
    static ScheduledFuture<?> t;
    private int pojoCount = 4;
    private int maxVersions = 500000;
    private boolean dump = true;

    private long timeoutInSeconds = 10;

    static class MyTask implements Runnable {
        private boolean dump;
        private int pojoCount;
        private BarbelHisto<DefaultPojo> core;

        public MyTask(BarbelHisto<DefaultPojo> core, boolean dump, int maxVersions, int pojoCount) {
            this.core = core;
            this.dump = dump;
            this.pojoCount = pojoCount;

        }

        @Override
        public void run() {
            List<DefaultPojo> pojos = EnhancedRandom.randomListOf(pojoCount, DefaultPojo.class);
            String id = "someId";
            for (DefaultPojo pojo : pojos) {
                pojo.setDocumentId(id);
            }
            long time = new Date().getTime();
            System.out.println("######### Pre-fetching #########");
            System.out.println("Core size before first insert: " + ((BarbelHistoCore<DefaultPojo>) core).size());
            boolean first = true;
            for (Object pojo : pojos) {
                core.save((DefaultPojo) pojo,
                        BarbelTestHelper.randomLocalDate(LocalDate.of(2010, 1, 1), LocalDate.of(2015, 1, 1)),
                        BarbelTestHelper.randomLocalDate(LocalDate.of(2015, 1, 2), LocalDate.of(2020, 1, 1)));
                if (first) {
                    System.out.println("Core size after first insert: " + ((BarbelHistoCore<DefaultPojo>) core).size());
                    first = false;
                }
            }
            System.out.println("######### Barbel-Statistics #########");
            BigDecimal timetaken = new BigDecimal((new Date().getTime() - time)).divide(new BigDecimal(1000))
                    .round(new MathContext(4, RoundingMode.HALF_UP));
            System.out.println("inserterted " + pojoCount + " in " + timetaken + " s");
            System.out.println("per object: " + new BigDecimal(new Date().getTime() - time)
                    .divide(new BigDecimal(pojoCount)).round(new MathContext(4, RoundingMode.HALF_UP)) + " ms");
            printBarbelStatitics();
            if (dump) {
                ((BarbelHistoCore<DefaultPojo>) core).unloadAll();
                System.out.println("Core dumped ----> o");
            }
            printMemory();
        }

        @SuppressWarnings("rawtypes")
        private void printBarbelStatitics() {
            int size = ((BarbelHistoCore) core).size();
            System.out.println("count of versions: " + size);
        }

        private void printMemory() {
            int mb = 1024 * 1024;
            Runtime runtime = Runtime.getRuntime();
            runtime.gc();
            System.out.println("##### Heap utilization statistics [MB] #####");
            System.out.println("Used Memory:" + (runtime.totalMemory() - runtime.freeMemory()) / mb);
            System.out.println("Free Memory:" + runtime.freeMemory() / mb);
            System.out.println("Total Memory:" + runtime.totalMemory() / mb);
            System.out.println("Max Memory:" + runtime.maxMemory() / mb);
        }
    }

    @Test
    public void testMemory() throws InterruptedException, ExecutionException, TimeoutException {
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        Logger rootLogger = loggerContext.getLogger("org.mongodb.driver");
        rootLogger.setLevel(Level.OFF);
        FlapDoodleEmbeddedMongo.instance();
        SimpleMongoListenerClient client = SimpleMongoListenerClient.create("mongodb://localhost:12345");
        SimpleMongoUpdateListener updateListener = SimpleMongoUpdateListener.create(client.getMongoClient(), "testDb",
                "testCol", DefaultPojo.class, BarbelHistoContext.getDefaultGson());
        SimpleMongoLazyLoadingListener loadingListener = SimpleMongoLazyLoadingListener.create(client.getMongoClient(), "testDb",
                "testCol", DefaultPojo.class, BarbelHistoContext.getDefaultGson());
        BarbelHisto<DefaultPojo> core = BarbelHistoBuilder.barbel().withSynchronousEventListener(updateListener)
                .withSynchronousEventListener(loadingListener).build();
        t = executor.scheduleAtFixedRate(new MyTask(core, dump, maxVersions, pojoCount), 0, 2, TimeUnit.SECONDS);
        assertThrows(TimeoutException.class, () -> t.get(timeoutInSeconds, TimeUnit.SECONDS));
        executor.shutdownNow();
    }

}