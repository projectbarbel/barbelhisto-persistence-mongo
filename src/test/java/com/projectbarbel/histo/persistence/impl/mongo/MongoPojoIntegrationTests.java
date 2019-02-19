package com.projectbarbel.histo.persistence.impl.mongo;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.Filters;
import com.projectbarbel.histo.persistence.mongo.DefaultMongoValueObject;

public class MongoPojoIntegrationTests {

    private static FlapDoodleEmbeddedMongo _mongo = FlapDoodleEmbeddedMongo.instance();
    private static MongoClient client = _mongo.client();
	private MongoCollection<DefaultMongoValueObject> col;

	@Before
	public void setUp() {
	    client.getDatabase("test").drop();
	    client.getDatabase("test").createCollection("testCol", new CreateCollectionOptions().capped(false));
		col = client.getDatabase("test").getCollection("testCol", DefaultMongoValueObject.class);
//		_mongo.getDatabase("test").createCollection("testCol", new CreateCollectionOptions().capped(false));
	}

	@Test
	public void testReadDocument_byData() {
		DefaultMongoValueObject object = BarbelTestHelper.random(DefaultMongoValueObject.class);
		col.insertOne(object);
		assertTrue("should be found by data", col.find(Filters.eq("data", object.getData())).iterator().hasNext());
	}

	@Test
	public void testReadDocument_byId() {
	    DefaultMongoValueObject object = BarbelTestHelper.random(DefaultMongoValueObject.class);
	    col.insertOne(object);
	    assertFalse("should not be found by id", col.find(Filters.eq("id", object.getVersionId())).iterator().hasNext());
	}
	
	@Test
	public void testReadDocument_by_id() {
	    DefaultMongoValueObject object = BarbelTestHelper.random(DefaultMongoValueObject.class);
	    col.insertOne(object);
	    assertTrue("should be found by _id", col.find(Filters.eq("objectId", object.getVersionId())).iterator().hasNext());
	}
	
}
