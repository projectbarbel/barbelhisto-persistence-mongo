package com.projectbarbel.histo.persistence.mongo;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.LocalDate;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.projectbarbel.histo.BarbelHisto;
import org.projectbarbel.histo.BarbelHistoBuilder;
import org.projectbarbel.histo.BarbelHistoContext;
import org.projectbarbel.histo.BarbelHistoCore;
import org.projectbarbel.histo.model.DefaultPojo;
import org.slf4j.LoggerFactory;

import com.projectbarbel.histo.persistence.impl.mongo.FlapDoodleEmbeddedMongo;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;

public class EmbeddedIntervalTest {

    @AfterAll
    public static void tearDown() {
        FlapDoodleEmbeddedMongo.destroy();
    }
    
    @BeforeAll
    public static void setUp() {
        FlapDoodleEmbeddedMongo.create();
    }
    
    @Test
    void embeddedOverlap() throws Exception {
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        Logger rootLogger = loggerContext.getLogger("org.mongodb.driver");
        rootLogger.setLevel(Level.OFF);
        SimpleMongoListenerClient client = SimpleMongoListenerClient.createFromProperties();
        client.getMongoClient().getDatabase("testDb").drop();
        SimpleMongoUpdateListener updateListener = SimpleMongoUpdateListener.create(client.getMongoClient(), "testDb",
                "testCol", DefaultPojo.class, BarbelHistoContext.getDefaultGson());
        SimpleMongoLazyLoadingListener loadingListener = SimpleMongoLazyLoadingListener.create(client.getMongoClient(), "testDb",
                "testCol", DefaultPojo.class, BarbelHistoContext.getDefaultGson());
        BarbelHisto<DefaultPojo> core = BarbelHistoBuilder.barbel().withSynchronousEventListener(updateListener)
                .withSynchronousEventListener(loadingListener).build();
        DefaultPojo pojo = new DefaultPojo("someSome", "some data");
        core.save(pojo, LocalDate.now(), LocalDate.MAX);
        pojo = new DefaultPojo("someSome", "changed");
        core.save(pojo, LocalDate.now().plusDays(1), LocalDate.now().plusDays(10));
        assertEquals(4,((BarbelHistoCore<DefaultPojo>)core).size());
        pojo = new DefaultPojo("someSome", "changed again");
        System.out.println(core.prettyPrintJournal("someSome"));
        core.save(pojo, LocalDate.now().plusDays(1), LocalDate.MAX);
        assertEquals(5,((BarbelHistoCore<DefaultPojo>)core).size());
        System.out.println(core.prettyPrintJournal("someSome"));
    }
    
    @Test
    void embeddedOverlap_notLocal() throws Exception {
        BarbelHisto<DefaultPojo> core = BarbelHistoBuilder.barbel().build();
        DefaultPojo pojo = new DefaultPojo("someSome", "some data");
        core.save(pojo, LocalDate.now(), LocalDate.now().plusDays(20));
        pojo = new DefaultPojo("someSome", "changed");
        core.save(pojo, LocalDate.now().plusDays(1), LocalDate.now().plusDays(10));
        assertEquals(4,((BarbelHistoCore<DefaultPojo>)core).size());
        pojo = new DefaultPojo("someSome", "changed again");
        System.out.println(core.prettyPrintJournal("someSome"));
        core.save(pojo, LocalDate.now().plusDays(1), LocalDate.now().plusDays(20));
        System.out.println(core.prettyPrintJournal("someSome"));
        assertEquals(5,((BarbelHistoCore<DefaultPojo>)core).size());
    }
}
