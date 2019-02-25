package com.projectbarbel.histo.persistence.mongo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;
import org.projectbarbel.histo.model.DefaultDocument;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.projectbarbel.histo.persistence.impl.mongo.FlapDoodleEmbeddedMongo;

public class MongoJournalStoreClientTest {

    @Test
        public void testCreateFromProperties() throws Exception {
            MongoJournalStoreClient instance = MongoJournalStoreClient.createFromProperties();
            assertNotNull(instance);
        }

    @Test
    public void testCreate() throws Exception {
        MongoJournalStoreClient instance = MongoJournalStoreClient.create("mongodb://localhost:12345", "db", "col");
        MongoClient client = instance.getMongoClient();
        MongoCollection<DefaultDocument> collection = client.getDatabase("db").getCollection("col", DefaultDocument.class);
        collection.insertOne(new DefaultDocument());
        assertEquals(1, collection.countDocuments());
    }

    @Test
        public void testCreateFromProperties_withFlapdoodle() throws Exception {
            FlapDoodleEmbeddedMongo.instance();
            MongoJournalStoreClient instance = MongoJournalStoreClient.createFromProperties();
            assertNotNull(instance);
        }

    @Test
    public void testProperties() throws Exception {
        assertEquals("mongodb://localhost:12345", MongoJournalStoreClient.properties("mongoprovider.properties").getProperty("com.projectbarbel.histo.persistence.mongo.host"));
        assertEquals("dfltdb", MongoJournalStoreClient.properties("mongoprovider.properties").getProperty("com.projectbarbel.histo.persistence.mongo.db"));
        assertEquals("dfltcol", MongoJournalStoreClient.properties("mongoprovider.properties").getProperty("com.projectbarbel.histo.persistence.mongo.col"));
    }

}
