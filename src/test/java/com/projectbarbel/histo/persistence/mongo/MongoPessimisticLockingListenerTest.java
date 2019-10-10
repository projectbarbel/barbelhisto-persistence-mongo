package com.projectbarbel.histo.persistence.mongo;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.ZonedDateTime;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.projectbarbel.histo.BarbelHisto;
import org.projectbarbel.histo.BarbelHistoBuilder;
import org.projectbarbel.histo.BarbelHistoCore;
import org.projectbarbel.histo.DocumentJournal;
import org.projectbarbel.histo.DocumentJournal.ProcessingState;
import org.projectbarbel.histo.event.EventType;
import org.projectbarbel.histo.event.EventType.AcquireLockEvent;
import org.projectbarbel.histo.event.EventType.BarbelInitializedEvent;
import org.projectbarbel.histo.event.EventType.ReleaseLockEvent;
import org.projectbarbel.histo.model.BitemporalStamp;
import org.projectbarbel.histo.model.DefaultDocument;
import org.projectbarbel.histo.model.DefaultPojo;
import org.projectbarbel.histo.model.EffectivePeriod;

import com.googlecode.cqengine.ConcurrentIndexedCollection;
import com.googlecode.cqengine.IndexedCollection;
import com.projectbarbel.histo.persistence.impl.mongo.FlapDoodleEmbeddedMongo;

public class MongoPessimisticLockingListenerTest {

    @AfterAll
    public static void tearDown() {
        FlapDoodleEmbeddedMongo.destroy();
    }
    
    @BeforeAll
    public static void setUp() {
        FlapDoodleEmbeddedMongo.create();
    }
    
    @Test
    public void testHandleInitialization() throws Exception {
        SimpleMongoListenerClient client = SimpleMongoListenerClient.createFromProperties();
        client.getMongoClient().getDatabase("lockDb").drop();
        MongoPessimisticLockingListener listener = MongoPessimisticLockingListener
                .create(client.getMongoClient(), "lockDb", "docLocks");
        BarbelHisto<DefaultPojo> histo = BarbelHistoBuilder.barbel().withSynchronousEventListener(listener).build();
        DefaultPojo pojo = new DefaultPojo("someId", "some data");
        histo.save(pojo, ZonedDateTime.now(), EffectivePeriod.INFINITE);
        assertEquals(1, ((BarbelHistoCore<DefaultPojo>) histo).size());
    }

    @Test
    public void testHandleInitializationLock_InsertAndRelease() throws Exception {
        SimpleMongoListenerClient client = SimpleMongoListenerClient.createFromProperties();
        client.getMongoClient().getDatabase("lockDb").drop();
        MongoPessimisticLockingListener listener = MongoPessimisticLockingListener
                .create(client.getMongoClient(), "lockDb", "docLocks");
        listener.handleInitialization((BarbelInitializedEvent) EventType.BARBELINITIALIZED.create());
        IndexedCollection<DefaultDocument> backbone = new ConcurrentIndexedCollection<DefaultDocument>();
        backbone.add(new DefaultDocument(BitemporalStamp.createActive("test"), "some data"));
        AcquireLockEvent event = (AcquireLockEvent) EventType.ACQUIRELOCK.create().with(DocumentJournal.class,
                DocumentJournal.create(ProcessingState.INTERNAL, BarbelHistoBuilder.barbel(), backbone, "test"));
        listener.handleAcuireLock(event);
        assertEquals(true, event.succeeded());
        ReleaseLockEvent release = (ReleaseLockEvent) EventType.RELEASELOCK.create().with(DocumentJournal.class,
                DocumentJournal.create(ProcessingState.INTERNAL, BarbelHistoBuilder.barbel(), backbone, "test"));
        listener.handleLockRelease(release);
        assertEquals(true, release.succeeded());
    }

    @Test
    public void testHandleInitializationLockFails() throws Exception {
        SimpleMongoListenerClient client = SimpleMongoListenerClient.createFromProperties();
        client.getMongoClient().getDatabase("lockDb").drop();
        MongoPessimisticLockingListener listener = MongoPessimisticLockingListener
                .create(client.getMongoClient(), "lockDb", "docLocks");
        listener.handleInitialization((BarbelInitializedEvent) EventType.BARBELINITIALIZED.create());
        IndexedCollection<DefaultDocument> backbone = new ConcurrentIndexedCollection<DefaultDocument>();
        backbone.add(new DefaultDocument(BitemporalStamp.createActive("test"), "some data"));
        AcquireLockEvent event = (AcquireLockEvent) EventType.ACQUIRELOCK.create().with(DocumentJournal.class,
                DocumentJournal.create(ProcessingState.INTERNAL, BarbelHistoBuilder.barbel(), backbone, "test"));
        listener.handleAcuireLock((AcquireLockEvent) EventType.ACQUIRELOCK.create().with(DocumentJournal.class,
                DocumentJournal.create(ProcessingState.INTERNAL, BarbelHistoBuilder.barbel(), backbone, "test")));
        listener.handleAcuireLock(event);
        assertEquals(false, event.succeeded());
    }

    @Test
    public void testHandleInitialization_releaseTwice() throws Exception {
        SimpleMongoListenerClient client = SimpleMongoListenerClient.createFromProperties();
        client.getMongoClient().getDatabase("lockDb").drop();
        MongoPessimisticLockingListener listener = MongoPessimisticLockingListener
                .create(client.getMongoClient(), "lockDb", "docLocks");
        listener.handleInitialization((BarbelInitializedEvent) EventType.BARBELINITIALIZED.create());
        IndexedCollection<DefaultDocument> backbone = new ConcurrentIndexedCollection<DefaultDocument>();
        backbone.add(new DefaultDocument(BitemporalStamp.createActive("test"), "some data"));
        ReleaseLockEvent release = (ReleaseLockEvent) EventType.RELEASELOCK.create().with(DocumentJournal.class,
                DocumentJournal.create(ProcessingState.INTERNAL, BarbelHistoBuilder.barbel(), backbone, "test"));
        listener.handleLockRelease(release);
        assertEquals(true, release.succeeded());
        listener.handleLockRelease(release);
        assertEquals(true, release.succeeded());
    }
    
}
