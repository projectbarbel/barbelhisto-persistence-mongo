package com.projectbarbel.histo.persistence.mongo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.ZonedDateTime;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.projectbarbel.histo.BarbelHisto;
import org.projectbarbel.histo.BarbelHistoBuilder;
import org.projectbarbel.histo.BarbelHistoContext;
import org.projectbarbel.histo.BarbelHistoCore;
import org.projectbarbel.histo.BarbelQueries;
import org.projectbarbel.histo.event.HistoEventFailedException;
import org.projectbarbel.histo.model.DefaultPojo;
import org.projectbarbel.histo.model.EffectivePeriod;

import com.google.gson.Gson;
import com.projectbarbel.histo.persistence.impl.mongo.FlapDoodleEmbeddedMongo;

@TestMethodOrder(OrderAnnotation.class)
public class SimpleMongoLazyLoadingListenerTest {

    @SuppressWarnings("unused")
    private static FlapDoodleEmbeddedMongo mongo;

    @BeforeAll
    public static void setUp() {
        mongo = FlapDoodleEmbeddedMongo.create();
        SimpleMongoListenerClient.createFromProperties().getMongoClient().getDatabase("testDb").drop();
    }

    @AfterAll
    public static void tearDown() {
        FlapDoodleEmbeddedMongo.destroy();
    }
    
    @Order(1)
    @Test
    public void testCreate() throws Exception {
        SimpleMongoLazyLoadingListener listener = SimpleMongoLazyLoadingListener.create(
                SimpleMongoListenerClient.createFromProperties().getMongoClient(), "testDb", "testCol",
                DefaultPojo.class, new Gson());
        assertNotNull(listener);
    }

    @Order(2)
    @Test
    public void testHandleRetrieveData() throws Exception {

        SimpleMongoListenerClient client = SimpleMongoListenerClient.create("mongodb://localhost:12345");
        SimpleMongoUpdateListener listener = SimpleMongoUpdateListener.create(client.getMongoClient(), "testDb",
                "testCol", DefaultPojo.class, BarbelHistoContext.getDefaultGson());
        BarbelHisto<DefaultPojo> histo = BarbelHistoBuilder.barbel().withSynchronousEventListener(listener).build();
        DefaultPojo pojo = new DefaultPojo("someId", "some data");
        histo.save(pojo, ZonedDateTime.now(), EffectivePeriod.INFINITE);
        assertEquals(1, client.getMongoClient().getDatabase("testDb").getCollection("testCol").count());
        histo.save(pojo, ZonedDateTime.now().plusDays(1), EffectivePeriod.INFINITE);
        assertEquals(3, client.getMongoClient().getDatabase("testDb").getCollection("testCol").count());

        SimpleMongoLazyLoadingListener lazyloader = SimpleMongoLazyLoadingListener.create(client.getMongoClient(),
                "testDb", "testCol", DefaultPojo.class, BarbelHistoContext.getDefaultGson());
        BarbelHisto<DefaultPojo> lazyHisto = BarbelHistoBuilder.barbel().withSynchronousEventListener(lazyloader)
                .build();
        DefaultPojo lazyPojo = lazyHisto.retrieveOne(BarbelQueries.effectiveNow("someId"));
        assertNotNull(lazyPojo, "must not be null");

    }

    @Order(3)
    @Test
    public void testHandleJournalInitialize() throws Exception {

        SimpleMongoListenerClient client = SimpleMongoListenerClient.create("mongodb://localhost:12345");
        SimpleMongoLazyLoadingListener lazyloader = SimpleMongoLazyLoadingListener.create(client.getMongoClient(),
                "testDb", "testCol", DefaultPojo.class, BarbelHistoContext.getDefaultGson());
        BarbelHisto<DefaultPojo> lazyHisto = BarbelHistoBuilder.barbel().withSynchronousEventListener(lazyloader)
                .build();
        lazyHisto.save(new DefaultPojo("someId", "changed data"), ZonedDateTime.now().minusDays(2), EffectivePeriod.INFINITE);
        assertEquals(4, ((BarbelHistoCore<DefaultPojo>) lazyHisto).size());

    }

    @Order(4)
    @Test
    public void testHandleRetrieveAll_throwsException() throws Exception {
        
        SimpleMongoListenerClient client = SimpleMongoListenerClient.create("mongodb://localhost:12345");
        SimpleMongoLazyLoadingListener lazyloader = SimpleMongoLazyLoadingListener.create(client.getMongoClient(),
                "testDb", "testCol", DefaultPojo.class, BarbelHistoContext.getDefaultGson());
        BarbelHisto<DefaultPojo> lazyHisto = BarbelHistoBuilder.barbel().withSynchronousEventListener(lazyloader)
                .build();
        assertThrows(HistoEventFailedException.class, ()->lazyHisto.retrieve(BarbelQueries.all()));        
    }
    
}
