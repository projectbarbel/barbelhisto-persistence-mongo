package com.projectbarbel.histo.persistence.impl.mongo;

import static org.junit.Assert.assertNotNull;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.projectbarbel.histo.persistence.mongo.impl.DefaultMongoValueObject;
import com.projectbarbel.histo.persistence.mongo.impl.MongoDocumentDaoImpl;
import com.projectbarbel.histo.persistence.util.BarbelTestHelper;

public class MongoDocumentDaoImpl_Read_IntegrationTest {

    private static MongoDocumentDaoImpl dao;

    @BeforeClass
    public static void beforeClass() {
        dao = new MongoDocumentDaoImpl(FlapDoodleEmbeddedMongoClientDaoSupplier.MONGOCLIENT.getMongo(), "test", "testCol");
    }

    @Before
    public void setUp() {
        dao.reset();
    }

    @Test
	public void testReadDocument() {
		DefaultMongoValueObject object = BarbelTestHelper.random(DefaultMongoValueObject.class);
		dao.createDocument(object);
		assertNotNull(dao.readDocument(object.getObjectId()).get());
    }

}
