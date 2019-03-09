package com.projectbarbel.histo.persistence.mongo;

import java.util.concurrent.TimeUnit;

import org.bson.Document;
import org.projectbarbel.histo.BarbelHisto;
import org.projectbarbel.histo.DocumentJournal;
import org.projectbarbel.histo.extension.AbstractLockingListener;

import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;

/**
 * Mongo locking listener implementation to lock document journals in the
 * backend. This facilitates mutliple {@link BarbelHisto} instances to work on a
 * single {@link MongoCollection}.<br>
 * 
 * @author Niklas Schlimm
 *
 */
public class MongoPessimisticLockingListener extends AbstractLockingListener {

    private static final String DOCUMENT_ID = "documentId";
    private MongoCollection<Document> lockCollection;
    private final MongoClient client;
    private final String dbName;
    private final String collectionName;

    private MongoPessimisticLockingListener(MongoClient client, String dbName, String collectionName) {
        this.client = client;
        this.dbName = dbName;
        this.collectionName = collectionName;
    }

    public static MongoPessimisticLockingListener create(MongoClient client, String dbName, String collectionName) {
        return new MongoPessimisticLockingListener(client, dbName, collectionName);
    }

    @Override
    public void doInitializeLockCollection() {
        lockCollection = client.getDatabase(dbName).getCollection(collectionName);
        lockCollection.createIndex(new BasicDBObject(DOCUMENT_ID, 1),
                new IndexOptions().expireAfter(3600L, TimeUnit.SECONDS).unique(true));
    }

    @Override
    public void doAcquireLock(DocumentJournal journal) {
        lockCollection.insertOne(new Document(DOCUMENT_ID, journal.getId()));
    }

    @Override
    public void doReleaseLock(DocumentJournal journal) {
        lockCollection.deleteOne(Filters.eq(DOCUMENT_ID, journal.getId()));
    }

}
