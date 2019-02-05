package com.projectbarbel.histo.persistence.mongo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import com.projectbarbel.histo.persistence.impl.mongo.FlapDoodleEmbeddedMongo;

public class MongoJournalStoreProviderTest {

    @Test
    public void testCreate() throws Exception {
        MongoJournalStoreProvider<DefaultMongoValueObject> instance = MongoJournalStoreProvider.create(DefaultMongoValueObject.class);
        assertNotNull(instance);
    }

    @Test
    public void testCreate_withFlapdoodle() throws Exception {
        FlapDoodleEmbeddedMongo.instance();
        MongoJournalStoreProvider<DefaultMongoValueObject> instance = MongoJournalStoreProvider.create(DefaultMongoValueObject.class);
        assertNotNull(instance);
    }

    @Test
    public void testProperties() throws Exception {
        assertEquals("mongodb://localhost:27017", MongoJournalStoreProvider.properties("mongoprovider.properties").getProperty("com.projectbarbel.histo.persistence.mongo.host"));
        assertEquals("dfltdb", MongoJournalStoreProvider.properties("mongoprovider.properties").getProperty("com.projectbarbel.histo.persistence.mongo.db"));
        assertEquals("dfltcol", MongoJournalStoreProvider.properties("mongoprovider.properties").getProperty("com.projectbarbel.histo.persistence.mongo.col"));
    }

}
