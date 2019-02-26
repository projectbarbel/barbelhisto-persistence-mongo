package com.projectbarbel.histo.persistence.mongo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.bson.Document;
import org.junit.Test;
import org.projectbarbel.histo.model.DefaultDocument;

import com.google.gson.Gson;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.projectbarbel.histo.persistence.impl.mongo.FlapDoodleEmbeddedMongo;

public class SimpleMongoListenerClientTest {

    @Test
    public void testCreateFromProperties() throws Exception {
        SimpleMongoListenerClient instance = SimpleMongoListenerClient.createFromProperties();
        assertNotNull(instance);
    }

    @Test
    public void testCreate() throws Exception {
        SimpleMongoListenerClient instance = SimpleMongoListenerClient.create("mongodb://localhost:12345");
        MongoClient client = instance.getMongoClient();
        MongoCollection<Document> collection = client.getDatabase("db").getCollection("col");
        collection.insertOne(Document.parse(new Gson().toJson(new DefaultDocument())));
        assertEquals(1, collection.countDocuments());
    }

    @Test
    public void testCreateFromProperties_withFlapdoodle() throws Exception {
        FlapDoodleEmbeddedMongo.instance();
        SimpleMongoListenerClient instance = SimpleMongoListenerClient.createFromProperties();
        assertNotNull(instance);
    }

    @Test
    public void testProperties() throws Exception {
        assertEquals("mongodb://localhost:12345", SimpleMongoListenerClient.properties("mongoprovider.properties")
                .getProperty("com.projectbarbel.histo.persistence.mongo.host"));
        assertEquals("dfltdb", SimpleMongoListenerClient.properties("mongoprovider.properties")
                .getProperty("com.projectbarbel.histo.persistence.mongo.db"));
        assertEquals("dfltcol", SimpleMongoListenerClient.properties("mongoprovider.properties")
                .getProperty("com.projectbarbel.histo.persistence.mongo.col"));
    }

}
