package com.projectbarbel.histo.persistence.mongo;

import static com.mongodb.client.model.Filters.eq;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.bson.Document;
import org.projectbarbel.histo.BarbelHisto;
import org.projectbarbel.histo.BarbelHistoCore;
import org.projectbarbel.histo.BarbelMode;
import org.projectbarbel.histo.BarbelQueries;
import org.projectbarbel.histo.DocumentJournal;
import org.projectbarbel.histo.event.EventType.BarbelInitializedEvent;
import org.projectbarbel.histo.event.EventType.InitializeJournalEvent;
import org.projectbarbel.histo.event.EventType.RetrieveDataEvent;
import org.projectbarbel.histo.model.Bitemporal;
import org.projectbarbel.histo.model.BitemporalVersion;

import com.google.common.eventbus.Subscribe;
import com.google.gson.Gson;
import com.googlecode.cqengine.query.Query;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;

/**
 * Mongo shadow lazy loading listener implementation to pre-fetch saved data
 * from previous sessions back to {@link BarbelHisto}. <br>
 * 
 * The simple listener implementations provide support for all
 * {@link BarbelQueries} and all custom queries as long they use the
 * {@link BarbelQueries#DOCUMENT_ID} as a filter criterion.
 * 
 * @author Niklas Schlimm
 *
 */
public class SimpleMongoLazyLoadingListener {

    private MongoCollection<Document> shadow;
    private final MongoClient client;
    private final String dbName;
    private final String collectionName;
    private final Class<?> managedType;
    private final Class<?> persistedType;
    private final BarbelMode mode;
    private String documentIdFieldName;
    private Gson gson;

    private SimpleMongoLazyLoadingListener(MongoClient client, String dbName, String collectionName,
            Class<?> managedType, Gson gson) {
        this.client = client;
        this.dbName = dbName;
        this.collectionName = collectionName;
        this.managedType = managedType;
        this.gson = gson;
        if (Bitemporal.class.isAssignableFrom(managedType))
            mode = BarbelMode.BITEMPORAL;
        else
            mode = BarbelMode.POJO;
        this.documentIdFieldName = mode.getDocumentIdFieldNameOnPersistedType(managedType);
        this.persistedType = mode.getPersistenceObjectType(managedType);
    }

    public static SimpleMongoLazyLoadingListener create(MongoClient client, String dbName, String collectionName,
            Class<?> managedType, Gson gson) {
        return new SimpleMongoLazyLoadingListener(client, dbName, collectionName, managedType, gson);
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
    public void handleRetrieveData(RetrieveDataEvent event) {
        try {
            Query<?> query = (Query<?>) event.getEventContext().get(RetrieveDataEvent.QUERY);
            BarbelHisto<?> histo = (BarbelHisto<?>) event.getEventContext().get(RetrieveDataEvent.BARBEL);
            final List<Object> ids = BarbelQueries.returnIDsForQuery(query, new ArrayList<>());
            if (!ids.isEmpty()) {
                for (Object id : ids) {
                    List<Bitemporal> docs = StreamSupport
                            .stream(shadow.find(eq(documentIdFieldName, id)).spliterator(), true)
                            .map(d -> (Bitemporal) toPersistedType((Document) d)).collect(Collectors.toList());
                    if (histo.contains(id))
                        histo.unload(id);
                    histo.load(docs);
                }
            } else {
                List<Bitemporal> docs = StreamSupport.stream(shadow.find().spliterator(), true)
                        .map(d -> (Bitemporal) toPersistedType((Document) d)).collect(Collectors.toList());
                if (((BarbelHistoCore<?>) histo).size() == 0)
                    histo.load(docs);
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
    public void handleInitializeJournal(InitializeJournalEvent event) {
        try {
            DocumentJournal journal = (DocumentJournal) event.getEventContext().get(DocumentJournal.class);
            BarbelHisto<?> histo = (BarbelHisto<?>) event.getEventContext().get(RetrieveDataEvent.BARBEL);
            List<Bitemporal> docs = StreamSupport
                    .stream(shadow.find(eq(documentIdFieldName, journal.getId())).spliterator(), true)
                    .map(d -> (Bitemporal) toPersistedType((Document) d)).collect(Collectors.toList());
            if (histo.contains(journal.getId()))
                histo.unload(journal.getId());
            histo.load(docs);
        } catch (Exception e) {
            event.failed(e);
        }
    }

}
