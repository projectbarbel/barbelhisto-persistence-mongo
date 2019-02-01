package com.projectbarbel.histo.persistence.impl.mongo;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.bson.types.ObjectId;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.projectbarbel.histo.persistence.mongo.DefaultMongoValueObject;
import com.projectbarbel.histo.persistence.mongo.DocumentDao;
import com.projectbarbel.histo.persistence.mongo.MongoDocumentDaoImpl;
import com.projectbarbel.histo.persistence.util.BarbelTestHelper;

public class MongoDocumentDaoImpl_Create_IntegrationTest {

    private static FlapDoodleEmbeddedMongo _mongo = FlapDoodleEmbeddedMongo.instance();
    private static MongoClient client = _mongo.client();
    
    private static DocumentDao<DefaultMongoValueObject, ObjectId> dao;
    private MongoCollection<DefaultMongoValueObject> col;

    @BeforeClass
    public static void setUpSuite() {
        dao = new MongoDocumentDaoImpl(client, "test", "testCol");
    }
    
    @Before
    public void setUp() {
        dao.reset();
        col = ((MongoDocumentDaoImpl)dao).getCol();
    }

    @Test
    public void testCreateDocument_and_reset() {
        DefaultMongoValueObject object = BarbelTestHelper.random(DefaultMongoValueObject.class);
        dao.createDocument(object);
        DefaultMongoValueObject doc = dao.readDocument(object.getVersionId()).get();
        assertNotNull(doc);
        dao.reset();
        doc = dao.readDocument(object.getVersionId()).orElse(null);
        assertNull(doc);
    }

    @Test(expected=MongoWriteException.class)
    public void testCreateDocument_and_CreateAgain_shouldThrowexception() {
        DefaultMongoValueObject object = BarbelTestHelper.random(DefaultMongoValueObject.class);
        dao.createDocument(object);
        DefaultMongoValueObject doc = dao.readDocument(object.getVersionId()).get();
        assertNotNull(doc);
        dao.createDocument(object);
    }
    
    @Test
    public void testCreateDocument_findNone_byEffectiveFrom() {
        DefaultMongoValueObject object = BarbelTestHelper.random(DefaultMongoValueObject.class);
        dao.createDocument(object);
        // try find one where stored effective date is equal to specified value, when
        // specified value is non equal
        DefaultMongoValueObject doc = col.find(Filters.eq("bitemporalStamp.effectiveFrom.seconds", 010101L)).first();
        assertNull(doc);
    }

}
