package com.projectbarbel.histo.persistence.mongo;

import static com.mongodb.client.model.Filters.eq;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.Validate;
import org.bson.Document;
import org.projectbarbel.histo.BarbelHisto;
import org.projectbarbel.histo.BarbelMode;
import org.projectbarbel.histo.DocumentJournal.Replacement;
import org.projectbarbel.histo.event.EventType.BarbelInitializedEvent;
import org.projectbarbel.histo.event.EventType.UpdateFinishedEvent;
import org.projectbarbel.histo.model.Bitemporal;
import org.projectbarbel.histo.model.BitemporalStamp;

import com.google.common.eventbus.Subscribe;
import com.google.gson.Gson;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.result.DeleteResult;

/**
 * Mongo shadow listener implementation to mirror {@link BarbelHisto} backbone
 * to {@link MongoCollection} and pre-fetch saved data from previous sessions
 * back to {@link BarbelHisto}. No persistent locking is applied, so
 * applications need to share a single instance of {@link BarbelHisto} using
 * this listener.<br>
 * <br>
 * 
 * Since the persisted type is computed at runtime depending on the
 * {@link BarbelMode} the class uses raw types.
 * 
 * @author Niklas Schlimm
 *
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class SimpleMongoUpdateListener {

    private static final String VERSION_ID = ".versionId";
    private MongoCollection shadow;
    private final MongoClient client;
    private final String dbName;
    private final String collectionName;
    private final BarbelMode mode;
    private String versionIdFieldName;
    private final Gson gson;

    public SimpleMongoUpdateListener(MongoClient client, String dbName, String collectionName, 
            Class managedType, Gson gson) {
        this.client = client;
        this.dbName = dbName;
        this.collectionName = collectionName;
        this.gson = gson;
        if (Bitemporal.class.isAssignableFrom(managedType)) {
            mode = BarbelMode.BITEMPORAL;
        } else {
            mode = BarbelMode.POJO;
        }
        this.versionIdFieldName = mode.getStampFieldName(mode.getPersistenceObjectType(managedType),
                BitemporalStamp.class) + VERSION_ID;
    }

    public static SimpleMongoUpdateListener create(MongoClient client, String dbName, String collectionName,
            Class managedType, Gson gson) {
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
    public void handleUpdate(UpdateFinishedEvent event) {
        try {
            List<Bitemporal> managedBitemporalsInserted = (List<Bitemporal>) event.getEventContext()
                    .get(UpdateFinishedEvent.NEWVERSIONS);
            Set<Replacement> replacements = (Set<Replacement>) event.getEventContext()
                    .get(UpdateFinishedEvent.REPLACEMENTS);
            // delete first ! cause version id is the key for deletion, and replaced new objects carry same version IDs
            List<Bitemporal> objectsRemoved = replacements.stream().flatMap(r -> r.getObjectsRemoved().stream())
                    .map(v -> mode.managedBitemporalToCustomPersistenceObject(v)).collect(Collectors.toList());
            List<DeleteResult> results = (List<DeleteResult>) objectsRemoved.stream()
                    .map(objectToRemove -> (DeleteResult) delete(objectToRemove)).collect(Collectors.toList());
            Validate.isTrue(results.stream().filter(r -> r.getDeletedCount() != 1).count() == 0,
                    "no valid delete results");
            // // @formatter:off
            List<Document> documentsToInsert = managedBitemporalsInserted.stream()
                    .map(v -> mode.managedBitemporalToCustomPersistenceObject(v))   // to persistence object
                    .map(v -> gson.toJson(v))                                       // to json
                    .map(v -> Document.parse(v))                                    // to mongo Document
                    .collect(Collectors.toList());                                  // to list
            List<Document> documentsAddedOnReplacements = replacements.stream()
                    .flatMap(r -> r.getObjectsAdded().stream())                     // to stream of managed objects
                    .map(v -> mode.managedBitemporalToCustomPersistenceObject(v))   // to persistence objects
                    .map(v -> gson.toJson(v))                                       // to json
                    .map(v -> Document.parse(v))                                    // to mongo Document
                    .collect(Collectors.toList());                                  // to list
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