package com.projectbarbel.histo.persistence.mongo;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.projectbarbel.histo.BarbelHisto;
import org.projectbarbel.histo.BarbelHistoBuilder;
import org.projectbarbel.histo.BarbelHistoContext;
import org.projectbarbel.histo.BarbelHistoCore;
import org.projectbarbel.histo.model.DefaultPojo;
import org.projectbarbel.histo.model.EffectivePeriod;
import org.slf4j.LoggerFactory;

import com.projectbarbel.histo.persistence.impl.mongo.FlapDoodleEmbeddedMongo;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;

public class TwoInstancesUseTimeshift {

    @AfterAll
    public static void tearDown() {
        FlapDoodleEmbeddedMongo.destroy();
    }
    
    @BeforeAll
    public static void setUp() {
        FlapDoodleEmbeddedMongo.create();
    }
    
    @Test
    void testSimultaneousUpdate_Retrieve() throws Exception {
        BarbelHistoContext.getBarbelClock().useFixedClockAt(ZonedDateTime.of(LocalDateTime.of(2019,2,28,8,0,0), ZoneId.systemDefault()));
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        Logger rootLogger = loggerContext.getLogger("org.mongodb.driver");
        rootLogger.setLevel(Level.OFF);
        SimpleMongoListenerClient client = SimpleMongoListenerClient.INSTANCE;
        client.getMongoClient().getDatabase("testDb").drop();
        SimpleMongoUpdateListener updateListener = SimpleMongoUpdateListener.create(client.getMongoClient(), "testDb",
                "testCol", DefaultPojo.class, BarbelHistoContext.getDefaultGson());
        SimpleMongoLazyLoadingListener loadingListener = SimpleMongoLazyLoadingListener.create(client.getMongoClient(), "testDb",
                "testCol", DefaultPojo.class, BarbelHistoContext.getDefaultGson());
        MongoPessimisticLockingListener locking = MongoPessimisticLockingListener.create(client.getMongoClient(), "lockDb", "docLocks");
        BarbelHisto<DefaultPojo> histo1 = BarbelHistoBuilder.barbel().withSynchronousEventListener(updateListener)
                .withSynchronousEventListener(loadingListener).withSynchronousEventListener(locking).build();
        BarbelHisto<DefaultPojo> histo2 = BarbelHistoBuilder.barbel().withSynchronousEventListener(updateListener)
                .withSynchronousEventListener(loadingListener).withSynchronousEventListener(locking).build();
        DefaultPojo pojo = new DefaultPojo("someId", "some data");
        ZonedDateTime zdt = ZonedDateTime.now();
        histo1.save(pojo, zdt, EffectivePeriod.INFINITE);
        histo2.save(pojo, zdt.plusDays(1), EffectivePeriod.INFINITE);
        assertEquals(1, ((BarbelHistoCore<DefaultPojo>)histo1).size());
        assertEquals(3, ((BarbelHistoCore<DefaultPojo>)histo2).size());
        // should load two active created by histo2
        BarbelHistoContext.getBarbelClock().useSystemDefaultZoneClock();
        assertEquals(2, histo1.timeshift("someId", ZonedDateTime.now().minusSeconds(1)).size());
    }
    
}
