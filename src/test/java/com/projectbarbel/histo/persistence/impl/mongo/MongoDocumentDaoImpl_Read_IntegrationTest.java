package com.projectbarbel.histo.persistence.impl.mongo;

import static org.junit.Assert.assertNotNull;

import org.junit.BeforeClass;
import org.junit.Test;

import com.mongodb.client.MongoClient;
import com.projectbarbel.histo.persistence.mongo.DefaultMongoValueObject;
import com.projectbarbel.histo.persistence.mongo.MongoDocumentDaoImpl;

public class MongoDocumentDaoImpl_Read_IntegrationTest {

    private static MongoDocumentDaoImpl dao;

    private static FlapDoodleEmbeddedMongo _mongo = FlapDoodleEmbeddedMongo.instance();
    private static MongoClient client = _mongo.client();
    
    @BeforeClass
    public static void setUpSuite() {
        dao = new MongoDocumentDaoImpl(client, "test", "testCol");
    }
    
    @Test
	public void testReadDocument() {
		DefaultMongoValueObject object = BarbelTestHelper.random(DefaultMongoValueObject.class);
		dao.createDocument(object);
		assertNotNull(dao.readDocument(object.getObjectId()).get());
    }

}
