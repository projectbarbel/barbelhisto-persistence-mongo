package com.projectbarbel.histo.persistence.mongo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.projectbarbel.histo.model.DefaultDocument;

import com.google.gson.Gson;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.projectbarbel.histo.persistence.impl.mongo.FlapDoodleEmbeddedMongo;

public class SimpleMongoListenerClientTest {

    @AfterAll
    public static void tearDown() {
        FlapDoodleEmbeddedMongo.destroy();
    }
    
    @BeforeAll
    public static void setUp() {
        FlapDoodleEmbeddedMongo.create();
    }
    
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
        assertEquals(1, collection.count());
    }

    @Test
    public void testCreateFromProperties_withFlapdoodle() throws Exception {
        SimpleMongoListenerClient instance = SimpleMongoListenerClient.createFromProperties();
        assertNotNull(instance);
    }

    @Test
    public void testProperties() throws Exception {
        assertNotNull(SimpleMongoListenerClient.properties("mongoprovider.properties")
                .getProperty("com.projectbarbel.histo.persistence.mongo.host"));
    }

}
