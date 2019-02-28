package com.projectbarbel.histo.persistence.mongo;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

/**
 * Class that creates the connection to the underlying journal store in MongoDB.
 * Optional use, clients can use their own mongo client configurations with the
 * listener instances.
 * 
 * @author Niklas Schlimm
 *
 */
public class SimpleMongoListenerClient {

    private static final String HOSTPROPNAME = "com.projectbarbel.histo.persistence.mongo.host";
    private static final String DFLTCONFIGFILE = "mongoprovider.properties";

    private final MongoClient mongoClient;
    
    public final static SimpleMongoListenerClient INSTANCE = createFromProperties();

    private SimpleMongoListenerClient(MongoClient client) {
        this.mongoClient = client;
    }

    public static SimpleMongoListenerClient create(MongoClientSettings settings) {
        MongoClient client = MongoClients.create(settings);
        return new SimpleMongoListenerClient(client);
    }

    public static SimpleMongoListenerClient createFromProperties() {
        return create(properties(DFLTCONFIGFILE).getProperty(HOSTPROPNAME));
    }

    public static SimpleMongoListenerClient create(String hostName) {
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(hostName)).build();
        MongoClient client = MongoClients.create(settings);
        return new SimpleMongoListenerClient(client);
    }

    protected static Properties properties(String configFileName) {
        Properties appProps = new Properties();
        try {
            Path path = Paths.get(SimpleMongoListenerClient.class.getClassLoader().getResource(DFLTCONFIGFILE).toURI());
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

}
