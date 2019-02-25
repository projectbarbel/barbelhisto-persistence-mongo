package com.projectbarbel.histo.persistence.mongo;

import static com.mongodb.client.model.Filters.eq;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.commons.lang3.Validate;
import org.projectbarbel.histo.BarbelHisto;
import org.projectbarbel.histo.BarbelMode;
import org.projectbarbel.histo.DocumentJournal;
import org.projectbarbel.histo.event.EventType.BarbelInitializedEvent;
import org.projectbarbel.histo.event.EventType.InitializeJournalEvent;
import org.projectbarbel.histo.event.EventType.InsertBitemporalEvent;
import org.projectbarbel.histo.event.EventType.ReplaceBitemporalEvent;
import org.projectbarbel.histo.event.EventType.RetrieveDataEvent;
import org.projectbarbel.histo.model.Bitemporal;
import org.projectbarbel.histo.model.BitemporalStamp;

import com.google.common.eventbus.Subscribe;
import com.googlecode.cqengine.query.Query;
import com.googlecode.cqengine.query.simple.Equal;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.result.DeleteResult;

/**
 * Mongo shadow listener implementation to mirror {@link BarbelHisto} backbone
 * to {@link MongoCollection} and pre-fetch saved data from previous sessions
 * back to {@link BarbelHisto}. No persistent locking is applied, so applications
 * need to share a single instance of {@link BarbelHisto} using this
 * listener.<br>
 * <br>
 * 
 * Since the persisted type is computed at runtime depending on the
 * {@link BarbelMode} the class uses raw types and unchecked casts.
 * 
 * @author Niklas Schlimm
 *
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class SimpleMongoShadowListener {

    private MongoCollection shadow;
    private final MongoClient client;
    private final String dbName;
    private final String collectionName;
    private final Class<?> managedType;
    private final BarbelMode mode;
    private String versionIdFieldName;
    private String documentIdFieldName;

    public <T> SimpleMongoShadowListener(MongoClient client, String dbName, String collectionName, Class<T> managedType) {
        this.client = client;
        this.dbName = dbName;
        this.collectionName = collectionName;
        this.managedType = managedType;
        if (Bitemporal.class.isAssignableFrom(managedType))
            mode = BarbelMode.BITEMPORAL;
        else
            mode = BarbelMode.POJO;
        this.versionIdFieldName = drawVersionIdFieldName();
        this.documentIdFieldName = mode.getDocumentIdFieldName(managedType);
    }

    public static SimpleMongoShadowListener create(MongoClient client, String dbName, String collectionName,
            Class<?> managedType) {
        return new SimpleMongoShadowListener(client, dbName, collectionName, managedType);
    }

    private String drawVersionIdFieldName() {
        return mode.getStampFieldName(mode.getPersistenceObjectType(managedType), BitemporalStamp.class) + ".versionId";
    }

    @Subscribe
    public void handleInitialization(BarbelInitializedEvent event) {
        shadow = client.getDatabase(dbName).getCollection(collectionName, mode.getPersistenceObjectType(managedType));
    }

    @Subscribe
    public void handleInserts(InsertBitemporalEvent event) {
        List<Bitemporal> managedBitemporalsInserted = (List<Bitemporal>) event.getEventContext()
                .get(InsertBitemporalEvent.NEWVERSIONS);
        List<?> os = managedBitemporalsInserted.stream().map(v -> mode.managedBitemporalToCustomPersistenceObject(v))
                .collect(Collectors.toList());
        shadow.insertMany(os);
    }

    @Subscribe
    public void handleReplacements(ReplaceBitemporalEvent event) {
        List<?> managedBitemporalsAdded = (List<?>) event.getEventContext().get(ReplaceBitemporalEvent.OBJECTS_ADDED);
        List<?> managedBitemporalsRemoved = (List<?>) event.getEventContext()
                .get(ReplaceBitemporalEvent.OBJECTS_REMOVED);
        List<DeleteResult> results = managedBitemporalsRemoved.stream()
                .map(objectToRemove -> (DeleteResult) delete(objectToRemove)).collect(Collectors.toList());
        Validate.isTrue(results.stream().filter(r -> r.getDeletedCount() != 1).count() == 0, "no valid delete results");
        shadow.insertMany((List<Bitemporal>) managedBitemporalsAdded.stream()
                .map(o -> mode.managedBitemporalToCustomPersistenceObject((Bitemporal) o))
                .collect(Collectors.toList()));
    }

    private DeleteResult delete(Object objectToRemove) {
        return shadow
                .deleteOne(eq(versionIdFieldName, ((Bitemporal) objectToRemove).getBitemporalStamp().getVersionId()));
    }

    @Subscribe
    public void handleRetrieveData(RetrieveDataEvent event) {
        Query<?> query = (Query<?>) event.getEventContext().get(RetrieveDataEvent.QUERY);
        BarbelHisto histo = (BarbelHisto) event.getEventContext().get(RetrieveDataEvent.BARBEL);
        if (query instanceof Equal) {
            final String id = (String) ((Equal) query).getValue();
            List docs = (List) StreamSupport.stream(shadow.find(eq(documentIdFieldName, id)).spliterator(), true)
                    .collect(Collectors.toList());
            histo.load(docs);
        }
    }

    @Subscribe
    public void handleInitializeJournal(InitializeJournalEvent event) {
        DocumentJournal journal = (DocumentJournal) event.getEventContext().get(DocumentJournal.class);
        BarbelHisto histo = (BarbelHisto) event.getEventContext().get(RetrieveDataEvent.BARBEL);
        List docs = (List) StreamSupport
                .stream(shadow.find(eq(documentIdFieldName, journal.getId())).spliterator(), true)
                .collect(Collectors.toList());
        histo.load(docs);
    }

}
