package com.projectbarbel.histo.persistence.impl.mongo;

import static org.junit.Assert.assertNotNull;

import org.junit.Test;

public class ImportJsonTest {

    @Test
    public void importTest() throws Exception {
        FlapDoodleEmbeddedMongo.instance().testStartAndStopMongoImportAndMongod("src/test/resources/data.json", "testDb", "testCol");
        assertNotNull(FlapDoodleEmbeddedMongo.instance().client().getDatabase("testDb").getCollection("testCol").find().first());
    }
}
