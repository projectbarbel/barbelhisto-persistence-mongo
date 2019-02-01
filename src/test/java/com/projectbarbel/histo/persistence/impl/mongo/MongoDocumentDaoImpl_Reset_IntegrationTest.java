package com.projectbarbel.histo.persistence.impl.mongo;

import static org.junit.Assert.assertNotNull;

import java.util.NoSuchElementException;

import org.bson.types.ObjectId;
import org.junit.BeforeClass;
import org.junit.Test;

import com.mongodb.client.MongoClient;
import com.projectbarbel.histo.persistence.mongo.DefaultMongoValueObject;
import com.projectbarbel.histo.persistence.mongo.DocumentDao;
import com.projectbarbel.histo.persistence.mongo.MongoDocumentDaoImpl;
import com.projectbarbel.histo.persistence.util.BarbelTestHelper;

public class MongoDocumentDaoImpl_Reset_IntegrationTest {

	private static DocumentDao<DefaultMongoValueObject, ObjectId> dao;

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

	@Test(expected=NoSuchElementException.class)
	public void testCreateDocument_andReset() {
		DefaultMongoValueObject object = BarbelTestHelper.random(DefaultMongoValueObject.class);
		dao.createDocument(object);
		DefaultMongoValueObject doc = dao.readDocument(object.getVersionId()).get();
		assertNotNull(doc);
		dao.reset();
		dao.readDocument(object.getVersionId()).get();
	}

}
