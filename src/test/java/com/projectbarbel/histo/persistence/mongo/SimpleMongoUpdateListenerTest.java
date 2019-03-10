package com.projectbarbel.histo.persistence.mongo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.time.LocalDate;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.projectbarbel.histo.BarbelHisto;
import org.projectbarbel.histo.BarbelHistoBuilder;
import org.projectbarbel.histo.BarbelHistoContext;
import org.projectbarbel.histo.model.DefaultPojo;

import com.projectbarbel.histo.persistence.impl.mongo.FlapDoodleEmbeddedMongo;

public class SimpleMongoUpdateListenerTest {

    @BeforeAll
    public static void setUp() {
        FlapDoodleEmbeddedMongo.create();
        SimpleMongoListenerClient.createFromProperties().getMongoClient().getDatabase("testDb").drop();
    }
    
    @AfterAll
    public static void tearDown() {
        FlapDoodleEmbeddedMongo.destroy();
    }
    
    @Test
    public void testHandleUpdate() throws Exception {
        SimpleMongoListenerClient client = SimpleMongoListenerClient.create("mongodb://localhost:12345");
        client.getMongoClient().getDatabase("testDb").drop();
        SimpleMongoUpdateListener listener = SimpleMongoUpdateListener.create(client.getMongoClient(), "testDb",
                "testCol", DefaultPojo.class, BarbelHistoContext.getDefaultGson());
        BarbelHisto<DefaultPojo> histo = BarbelHistoBuilder.barbel().withSynchronousEventListener(listener).build();
        DefaultPojo pojo = new DefaultPojo("someId", "some data");
        histo.save(pojo, LocalDate.now(), LocalDate.MAX);
        assertEquals(1, client.getMongoClient().getDatabase("testDb").getCollection("testCol").count());
        histo.save(pojo, LocalDate.now().plusDays(1), LocalDate.MAX);
        assertEquals(3, client.getMongoClient().getDatabase("testDb").getCollection("testCol").count());
    }

    @Test
    public void testCreate() throws Exception {
        SimpleMongoListenerClient client = SimpleMongoListenerClient.create("mongodb://localhost:12345");
        SimpleMongoUpdateListener listener = SimpleMongoUpdateListener.create(client.getMongoClient(), "testDb",
                "testCol", DefaultPojo.class, BarbelHistoContext.getDefaultGson());
        assertNotNull(listener);
    }

}
