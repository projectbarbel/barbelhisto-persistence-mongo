package com.projectbarbel.histo.persistence.impl.mongo;

import static org.junit.Assert.assertNotNull;

import java.util.NoSuchElementException;

import org.bson.types.ObjectId;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.projectbarbel.histo.persistence.mongo.DocumentDao;
import com.projectbarbel.histo.persistence.mongo.impl.DefaultMongoValueObject;
import com.projectbarbel.histo.persistence.mongo.impl.MongoDocumentDaoImpl;
import com.projectbarbel.histo.persistence.util.BarbelTestHelper;

public class MongoDocumentDaoImpl_Reset_IntegrationTest {

	private static DocumentDao<DefaultMongoValueObject, ObjectId> dao;

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
