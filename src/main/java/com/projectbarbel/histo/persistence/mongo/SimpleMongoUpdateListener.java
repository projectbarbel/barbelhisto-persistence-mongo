package com.projectbarbel.histo.persistence.mongo;

import static com.mongodb.client.model.Filters.eq;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.commons.lang3.Validate;
import org.bson.Document;
import org.projectbarbel.histo.BarbelHisto;
import org.projectbarbel.histo.BarbelHistoCore;
import org.projectbarbel.histo.BarbelMode;
import org.projectbarbel.histo.DocumentJournal.Inactivation;
import org.projectbarbel.histo.event.EventType.BarbelInitializedEvent;
import org.projectbarbel.histo.event.EventType.OnLoadOperationEvent;
import org.projectbarbel.histo.event.EventType.UnLoadOperationEvent;
import org.projectbarbel.histo.event.EventType.UpdateFinishedEvent;
import org.projectbarbel.histo.model.Bitemporal;
import org.projectbarbel.histo.model.BitemporalStamp;
import org.projectbarbel.histo.model.BitemporalVersion;

import com.google.common.eventbus.Subscribe;
import com.google.gson.Gson;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.result.DeleteResult;

/**
 * Mongo shadow update listener implementation to synchronize
 * {@link BarbelHisto} backbone updates to {@link MongoCollection}.
 * 
 * @author Niklas Schlimm
 *
 */
public class SimpleMongoUpdateListener {

    private static final String VERSION_ID = ".versionId";
    private MongoCollection<Document> shadow;
    private final MongoClient client;
    private final String dbName;
    private final String collectionName;
    private final BarbelMode mode;
    private String versionIdFieldName;
    private final Gson gson;
    private final Class<?> managedType;
    private final String documentIdFieldName;
    private final Class<?> persistedType;

    public SimpleMongoUpdateListener(MongoClient client, String dbName, String collectionName, Class<?> managedType,
            Gson gson) {
        this.client = client;
        this.dbName = dbName;
        this.collectionName = collectionName;
        this.managedType = managedType;
        this.gson = gson;
        if (Bitemporal.class.isAssignableFrom(managedType)) {
            mode = BarbelMode.BITEMPORAL;
        } else {
            mode = BarbelMode.POJO;
        }
        this.versionIdFieldName = mode.getStampFieldName(mode.getPersistenceObjectType(managedType),
                BitemporalStamp.class) + VERSION_ID;
        this.documentIdFieldName = mode.getDocumentIdFieldNameOnPersistedType(managedType);
        this.persistedType = mode.getPersistenceObjectType(managedType);
    }

    public static SimpleMongoUpdateListener create(MongoClient client, String dbName, String collectionName,
            Class<?> managedType, Gson gson) {
        return new SimpleMongoUpdateListener(client, dbName, collectionName, managedType, gson);
    }

    @Subscribe
    public void handleInitialization(BarbelInitializedEvent event) {
        try {
            shadow = client.getDatabase(dbName).getCollection(collectionName);
        } catch (Exception e) {
            event.failed(e);
        }
    }

    @Subscribe
    public void handleLoadOperation(OnLoadOperationEvent event) {
        try {
            @SuppressWarnings("unchecked")
            Collection<Bitemporal> managedBitemporalsToLoad = (Collection<Bitemporal>) event.getEventContext()
                    .get(OnLoadOperationEvent.DATA);
            for (Bitemporal bitemporal : managedBitemporalsToLoad) {
                if (shadow.find(eq(documentIdFieldName, bitemporal.getBitemporalStamp().getDocumentId())).iterator()
                        .hasNext())
                    throw new IllegalStateException("document with id exists: "
                            + bitemporal.getBitemporalStamp().getDocumentId() + " - unable to load");
            }
            // @formatter:off
            List<Document> documentsToInsert = managedBitemporalsToLoad.stream()
                    .map(mode::managedBitemporalToCustomPersistenceObject) // to persistence object
                    .map(gson::toJson) // to json
                    .map(Document::parse) // to mongo Document
                    .collect(Collectors.toList()); // to list
            // @formatter:on
            shadow.insertMany(documentsToInsert);
        } catch (Exception e) {
            event.failed(e);
        }
    }

    @Subscribe
    public void handleUnLoadOperation(UnLoadOperationEvent event) {
        try {
            BarbelHisto<?> histo = (BarbelHisto<?>) event.getEventContext().get(UnLoadOperationEvent.BARBEL);
            Object[] documentIds = (Object[]) event.getEventContext().get(UnLoadOperationEvent.DOCUMENT_IDs);
            for (Object id : documentIds) {
                List<Bitemporal> docs = StreamSupport
                        .stream(shadow.find(eq(documentIdFieldName, id)).spliterator(), true)
                        .map(d -> (Bitemporal) toPersistedType((Document) d)).collect(Collectors.toList());
                if (histo.contains(id))
                    ((BarbelHistoCore<?>) histo).unloadInternal(id);
                ((BarbelHistoCore<?>) histo).loadQuiet(docs);
                shadow.deleteMany(eq(documentIdFieldName, id));
            }
        } catch (Exception e) {
            event.failed(e);
        }
    }

    private Object toPersistedType(Document document) {
        String json = document.toJson();
        Object object = gson.fromJson(json, persistedType);
        if (object instanceof BitemporalVersion) {
            BitemporalVersion bv = (BitemporalVersion) object;
            bv.setObject(gson.fromJson(gson.toJsonTree(bv.getObject()).toString(), managedType));
        }
        return object;
    }

    @Subscribe
    public void handleUpdate(UpdateFinishedEvent event) {
        try {
            @SuppressWarnings("unchecked")
            List<Bitemporal> managedBitemporalsInserted = (List<Bitemporal>) event.getEventContext()
                    .get(UpdateFinishedEvent.NEWVERSIONS);
            @SuppressWarnings("unchecked")
            Set<Inactivation> inactivations = (Set<Inactivation>) event.getEventContext()
                    .get(UpdateFinishedEvent.INACTIVATIONS);
            // delete first ! cause version id is the key for deletion, and replaced new
            // objects carry same version IDs
            List<Bitemporal> objectsRemoved = inactivations.stream()
                    .map(r -> mode.managedBitemporalToCustomPersistenceObject(r.getObjectRemoved()))
                    .collect(Collectors.toList());
            List<DeleteResult> results = objectsRemoved.stream()
                    .map(objectToRemove -> (DeleteResult) delete(objectToRemove)).collect(Collectors.toList());
            Validate.validState(results.stream().filter(r -> r.getDeletedCount() != 1).count() == 0,
                    "delete operation failed - delete count must always be = 1");
            // // @formatter:off
            List<Document> documentsToInsert = managedBitemporalsInserted.stream()
                    .map(mode::managedBitemporalToCustomPersistenceObject) // to persistence object
                    .map(gson::toJson) // to json
                    .map(Document::parse) // to mongo Document
                    .collect(Collectors.toList()); // to list
            List<Document> documentsAddedOnReplacements = inactivations.stream()
                    .map(r -> mode.managedBitemporalToCustomPersistenceObject(r.getObjectAdded())) // to persistence objects
                    .map(gson::toJson) // to json
                    .map(Document::parse) // to mongo Document
                    .collect(Collectors.toList()); // to list
            documentsToInsert.addAll(documentsAddedOnReplacements);
            // @formatter:on
            shadow.insertMany(documentsToInsert);
        } catch (Exception e) {
            event.failed(e);
        }
    }

    private DeleteResult delete(Object objectToRemove) {
        return shadow
                .deleteOne(eq(versionIdFieldName, ((Bitemporal) objectToRemove).getBitemporalStamp().getVersionId()));
    }

}
