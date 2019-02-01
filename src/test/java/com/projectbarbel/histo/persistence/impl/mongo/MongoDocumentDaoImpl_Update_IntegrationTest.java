package com.projectbarbel.histo.persistence.impl.mongo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.util.Optional;

import org.bson.types.ObjectId;
import org.junit.BeforeClass;
import org.junit.Test;

import com.mongodb.client.MongoClient;
import com.projectbarbel.histo.persistence.mongo.DefaultMongoValueObject;
import com.projectbarbel.histo.persistence.mongo.MongoDocumentDaoImpl;
import com.projectbarbel.histo.persistence.util.BarbelTestHelper;

public class MongoDocumentDaoImpl_Update_IntegrationTest {

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
	public void testUpdateDocument_updateData() {
	    DefaultMongoValueObject object = BarbelTestHelper.random(DefaultMongoValueObject.class);
		Optional<ObjectId> oid = dao.createDocument(object);
		DefaultMongoValueObject doc = dao.readDocument(oid.get()).get();
		DefaultMongoValueObject changedObj = new DefaultMongoValueObject(object.getVersionId(), object.getBitemporalStamp(), "new data");
		Optional<ObjectId> newId = dao.updateDocument(oid.orElse(new ObjectId()), changedObj);
		DefaultMongoValueObject updatedObj = dao.readDocument(newId.orElse(new ObjectId())).get();
		assertEquals(changedObj, updatedObj);
		assertTrue(updatedObj.getData().equals("new data"));
		assertNotEquals(updatedObj, doc);
	}

	@Test
	public void testUpdateDocument_noUpdates() {
	    DefaultMongoValueObject object = BarbelTestHelper.random(DefaultMongoValueObject.class);
	    Optional<ObjectId> oid = dao.createDocument(object);
		Optional<ObjectId> updatedOid = dao.updateDocument(oid.orElse(new ObjectId()), object);
		DefaultMongoValueObject updatedDoc = dao.readDocument(updatedOid.orElse(new ObjectId())).get();
		assertEquals(updatedDoc.getData(), object.getData());
		assertEquals(updatedDoc, object);
	}

}
