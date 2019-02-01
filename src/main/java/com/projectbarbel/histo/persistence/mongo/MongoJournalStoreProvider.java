package com.projectbarbel.histo.persistence.mongo;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.projectbarbel.histo.persistence.api.DocumentJournal;
import com.projectbarbel.histo.persistence.api.JournalStoreProvider;

public class MongoJournalStoreProvider<T> implements JournalStoreProvider<T> {

    private static final String HOSTPROPNAME = "com.projectbarbel.histo.persistence.mongo.host";
    private static final String DFLTDBNAME = "com.projectbarbel.histo.persistence.mongo.db";
    private static final String DFLTCOLNAME = "com.projectbarbel.histo.persistence.mongo.col";
    private static final String DFLTCONFIGFILE = "mongoprovider.properties";

    private final MongoClient mongoClient;
    private final String dfltDbName;
    private final String dfltColName;
    private Class<T> journalType;
    @SuppressWarnings("unused")
    private String stampFieldName;

    private MongoJournalStoreProvider(Class<T> journalType, MongoClient client, String dfltName, String dfltColName) {
        this.mongoClient = client;
        this.dfltDbName = dfltName;
        this.dfltColName = dfltColName;
        this.journalType = journalType;
        this.stampFieldName = determineStampFieldName(journalType);
    }

    private String determineStampFieldName(Class<T> journalType) {
        return "";
    }

    @SuppressWarnings("unchecked")
    public static <T extends MongoJournalStoreProvider<O>, O> T create(Class<O> journalType, MongoClientSettings settings, String databaseName, String collectionName) {
        MongoClient client = MongoClients.create(settings);
        return (T)new MongoJournalStoreProvider<O>(journalType, client, databaseName, collectionName);
    }

    @SuppressWarnings("unchecked")
    public static <T extends MongoJournalStoreProvider<O>, O> T create(Class<O> journalType) {
        CodecRegistry registry = fromRegistries(CodecRegistries.fromCodecs(new BitemporalCodec()),
                MongoClientSettings.getDefaultCodecRegistry(),
                fromProviders(PojoCodecProvider.builder().automatic(true).build()));
        MongoClientSettings settings = MongoClientSettings.builder().codecRegistry(registry)
                .applyConnectionString(new ConnectionString(properties(DFLTCONFIGFILE).getProperty(HOSTPROPNAME)))
                .build();
        String dfltDbName = properties(DFLTCONFIGFILE).getProperty(DFLTDBNAME);
        String dfltColName = properties(DFLTCONFIGFILE).getProperty(DFLTCOLNAME);
        MongoClient client = MongoClients.create(settings);
        return (T) new MongoJournalStoreProvider<O>(journalType, client, dfltDbName, dfltColName);
    }

    protected static Properties properties(String configFileName) {
        String rootPath = Thread.currentThread().getContextClassLoader().getResource("").getPath();
        String appConfigPath = rootPath + configFileName;
        Properties appProps = new Properties();
        try {
            appProps.load(new FileInputStream(appConfigPath));
        } catch (FileNotFoundException e) {
            throw new IllegalArgumentException("the config file name could not be found: " + configFileName, e);
        } catch (IOException e) {
            throw new IllegalArgumentException("config file i/o failed for config file: " + configFileName, e);
        }
        return appProps;
    }

    public DocumentJournal<T> loadJournal(String documentId) {
        @SuppressWarnings("unused")
        MongoCollection<T> col = mongoClient.getDatabase(dfltDbName).getCollection(dfltColName, journalType); 
        return null;
    }

    @Override
    public long persistJournal(DocumentJournal<T> journal) {
        return 0;
    }

}
