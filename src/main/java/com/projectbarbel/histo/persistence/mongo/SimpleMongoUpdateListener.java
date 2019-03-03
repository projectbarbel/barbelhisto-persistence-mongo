package com.projectbarbel.histo.persistence.mongo;

import static com.mongodb.client.model.Filters.eq;

import java.util.List;

import org.bson.Document;
import org.projectbarbel.histo.extension.AbstractMongoUpdateListener;

import com.google.gson.Gson;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;

public class SimpleMongoUpdateListener extends AbstractMongoUpdateListener<MongoCollection<Document>, Document>{
    private final MongoClient client;
    private final String dbName;
    private final String collectionName;

    public SimpleMongoUpdateListener(Class<?> managedType, Gson gson, MongoClient client, String dbName,
            String collectionName) {
        super(managedType, gson);
        this.client = client;
        this.dbName = dbName;
        this.collectionName = collectionName;
    }


    @Override
    public MongoCollection<Document> createResource() {
        return client.getDatabase(dbName).getCollection(collectionName);
    }


    @Override
    public long delete(String versionId) {
        return shadow
                .deleteOne(eq(versionIdFieldName, versionId)).getDeletedCount();
    }

    @Override
    public long deleteJournal(Object id) {
        return shadow.deleteMany(eq(documentIdFieldName, id)).getDeletedCount();
    }

    public static SimpleMongoUpdateListener create(MongoClient client, String dbName, String collectionName,
            Class<?> managedType, Gson gson) {
        return new SimpleMongoUpdateListener(managedType, gson, client, dbName, collectionName);
    }

    @Override
    public void insertDocuments(List<Document> documentsToInsert) {
        shadow.insertMany(documentsToInsert);
    }

    @Override
    public FindIterable<Document> queryJournal(Object id) {
        return shadow.find(eq(documentIdFieldName, id));
    }
    @Override
    public Document fromJsonToStoredDocument(String json) {
        return Document.parse(json);
    }

    @Override
    public String fromStroredDocumentToJson(Document document) {
        return document.toJson();
    }

}
