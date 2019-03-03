package com.projectbarbel.histo.persistence.mongo;

import static com.mongodb.client.model.Filters.eq;

import org.bson.Document;
import org.projectbarbel.histo.extension.AbstractLazyLoadingListener;
import org.projectbarbel.histo.model.Bitemporal;
import org.projectbarbel.histo.model.BitemporalVersion;

import com.google.gson.Gson;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;

public class SimpleMongoLazyLoadingListener extends AbstractLazyLoadingListener<MongoCollection<Document>, Document> {

    private final MongoClient client;
    private final String dbName;
    private final String collectionName;
    
    public SimpleMongoLazyLoadingListener(MongoClient client, String dbName, String collectionName, Class<?> managedType, Gson gson,
            boolean singletonContext) {
        super(managedType, gson, singletonContext);
        this.client = client;
        this.dbName = dbName;
        this.collectionName = collectionName;
    }

    public Iterable<Document> queryAll() {
        return shadow.find();
    }

    public Iterable<Document> queryJournal(Object id) {
        return shadow.find(eq(documentIdFieldName, id));
    }

    public MongoCollection<Document> createResource() {
        return client.getDatabase(dbName).getCollection(collectionName);
    }

    public Bitemporal toPersistedType(Document document) {
        String json = document.toJson();
        Object object = gson.fromJson(json, persistedType);
        if (object instanceof BitemporalVersion) {
            BitemporalVersion bv = (BitemporalVersion) object;
            bv.setObject(gson.fromJson(gson.toJsonTree(bv.getObject()).toString(), managedType));
        }
        return (Bitemporal)object;
    }

    public static SimpleMongoLazyLoadingListener create(MongoClient client, String dbName, String collectionName,
            Class<?> managedType, Gson gson, boolean singletonContext) {
        return new SimpleMongoLazyLoadingListener(client, dbName, collectionName, managedType, gson, singletonContext);
    }

    public static SimpleMongoLazyLoadingListener create(MongoClient client, String dbName, String collectionName,
            Class<?> managedType, Gson gson) {
        return new SimpleMongoLazyLoadingListener(client, dbName, collectionName, managedType, gson, false);
    }

}
