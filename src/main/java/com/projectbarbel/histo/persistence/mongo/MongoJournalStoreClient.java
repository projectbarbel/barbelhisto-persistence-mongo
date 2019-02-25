package com.projectbarbel.histo.persistence.mongo;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

/**
 * Class that creates the connection to the underlying journal store in mongo.
 * 
 * @author Niklas Schlimm
 *
 * @param <T> the managed type
 */
public class MongoJournalStoreClient<T> {

    private static final String HOSTPROPNAME = "com.projectbarbel.histo.persistence.mongo.host";
    private static final String DFLTDBNAME = "com.projectbarbel.histo.persistence.mongo.db";
    private static final String DFLTCOLNAME = "com.projectbarbel.histo.persistence.mongo.col";
    private static final String DFLTCONFIGFILE = "mongoprovider.properties";

    private final MongoClient mongoClient;
    private final Class<T> journalType;

    private MongoJournalStoreClient(Class<T> journalType, MongoClient client) {
        this.mongoClient = client;
        this.journalType = journalType;
    }

    @SuppressWarnings("unchecked")
    public static <T extends MongoJournalStoreClient<O>, O> T create(Class<O> journalType,
            MongoClientSettings settings) {
        MongoClient client = MongoClients.create(settings);
        return (T) new MongoJournalStoreClient<O>(journalType, client);
    }

    public static <T extends MongoJournalStoreClient<O>, O> T createFromProperties(Class<O> journalType) {
        return create(journalType, properties(DFLTCONFIGFILE).getProperty(HOSTPROPNAME),
                properties(DFLTCONFIGFILE).getProperty(DFLTDBNAME),
                properties(DFLTCONFIGFILE).getProperty(DFLTCOLNAME));
    }

    @SuppressWarnings("unchecked")
    public static <T extends MongoJournalStoreClient<O>, O> T create(Class<O> journalType, String hostName,
            String dfltDbName, String dfltColName) {
        CodecRegistry registry = fromRegistries(CodecRegistries.fromCodecs(new BitemporalCodec()),
                MongoClientSettings.getDefaultCodecRegistry(),
                fromProviders(PojoCodecProvider.builder().automatic(true).build()));
        MongoClientSettings settings = MongoClientSettings.builder().codecRegistry(registry)
                .applyConnectionString(new ConnectionString(hostName)).build();
        MongoClient client = MongoClients.create(settings);
        return (T) new MongoJournalStoreClient<O>(journalType, client);
    }

    protected static Properties properties(String configFileName) {
        Properties appProps = new Properties();
        try {
            Path path = Paths.get(MongoJournalStoreClient.class.getClassLoader().getResource(DFLTCONFIGFILE).toURI());
            appProps.load(Files.newBufferedReader(path));
        } catch (FileNotFoundException e) {
            throw new IllegalArgumentException("the config file name could not be found: " + configFileName, e);
        } catch (IOException e) {
            throw new IllegalArgumentException("config file i/o failed for config file: " + configFileName, e);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("could not generate URI, worn syntax for config file: " + configFileName,
                    e);
        }
        return appProps;
    }

    public MongoClient getMongoClient() {
        return mongoClient;
    }

    public Class<T> getJournalType() {
        return journalType;
    }

}
