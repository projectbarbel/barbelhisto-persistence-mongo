package com.projectbarbel.histo.persistence.impl.mongo;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.util.Optional;

import org.bson.types.ObjectId;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.projectbarbel.histo.persistence.mongo.impl.DefaultMongoValueObject;
import com.projectbarbel.histo.persistence.mongo.impl.MongoDocumentDaoImpl;
import com.projectbarbel.histo.persistence.util.BarbelTestHelper;

public class MongoDocumentDaoImpl_Delete_IntegrationTest {

    private static MongoDocumentDaoImpl dao;
    
    @BeforeClass
    public static void beforeClass() {
        dao = new MongoDocumentDaoImpl(FlapDoodleEmbeddedMongoClientDaoSupplier.MONGOCLIENT.getMongo(), "test", "testCol");
    }

    @Before
    public void setUp() {
        dao.reset();
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
