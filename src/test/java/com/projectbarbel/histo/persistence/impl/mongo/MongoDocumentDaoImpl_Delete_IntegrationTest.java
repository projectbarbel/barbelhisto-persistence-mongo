package com.projectbarbel.histo.persistence.impl.mongo;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.util.Optional;

import org.bson.types.ObjectId;
import org.junit.BeforeClass;
import org.junit.Test;

import com.mongodb.client.MongoClient;
import com.projectbarbel.histo.persistence.mongo.DefaultMongoValueObject;
import com.projectbarbel.histo.persistence.mongo.MongoDocumentDaoImpl;

public class MongoDocumentDaoImpl_Delete_IntegrationTest {

    private static MongoDocumentDaoImpl dao;
    
    private static FlapDoodleEmbeddedMongo _mongo = FlapDoodleEmbeddedMongo.instance();
    private static MongoClient client = _mongo.client();
    
    @BeforeClass
    public static void setUpSuite() {
        dao = new MongoDocumentDaoImpl(client, "test", "testCol");
    }
    
	@Test(expected = NullPointerException.class)
	public void testCreateDocument_null() {
		dao.createDocument(null);
	}

	@Test
	public void testDeleteDocument() {
		DefaultMongoValueObject object = BarbelTestHelper.random(DefaultMongoValueObject.class);
		Optional<ObjectId> oid = dao.createDocument(object);
		DefaultMongoValueObject doc = dao.readDocument(oid.get()).orElse(null);
		assertNotNull(doc);
		dao.deleteDocument(oid.get());
		boolean hasnext = dao.readDocument(oid.get()).isPresent();
		assertFalse(hasnext);
	}

}
