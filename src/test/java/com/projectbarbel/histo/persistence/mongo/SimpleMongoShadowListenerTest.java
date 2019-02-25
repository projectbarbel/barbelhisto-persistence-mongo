package com.projectbarbel.histo.persistence.mongo;

import static org.junit.Assert.assertNotNull;

import org.junit.Test;
import org.projectbarbel.histo.model.DefaultPojo;

import com.projectbarbel.histo.persistence.impl.mongo.FlapDoodleEmbeddedMongo;

public class SimpleMongoShadowListenerTest {

    @Test
    public void testCreate() throws Exception {
        SimpleMongoShadowListener listener = SimpleMongoShadowListener.create(FlapDoodleEmbeddedMongo.instance().client(), "testDb",
                "testCol", DefaultPojo.class);
        assertNotNull(listener);
    }
}
