package com.projectbarbel.histo.persistence.impl.mongo;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

import java.io.IOException;
import java.net.UnknownHostException;

import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.projectbarbel.histo.persistence.mongo.BitemporalCodec;

import de.flapdoodle.embed.mongo.MongoImportExecutable;
import de.flapdoodle.embed.mongo.MongoImportProcess;
import de.flapdoodle.embed.mongo.MongoImportStarter;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.IMongoImportConfig;
import de.flapdoodle.embed.mongo.config.MongoImportConfigBuilder;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.runtime.Network;

public class FlapDoodleEmbeddedMongo {

    private static final MongodStarter starter = MongodStarter.getDefaultInstance();
    private static MongodExecutable _mongodExe;
    private static MongodProcess _mongod;

    private static FlapDoodleEmbeddedMongo MONGOSERVER;

    /**
     * Thread unsafe singleton method for tests.
     * 
     * @return the mongo instance
     */
    public static FlapDoodleEmbeddedMongo instance() {
        try {
            if (MONGOSERVER == null) {
                _mongodExe = starter.prepare(new MongodConfigBuilder().version(Version.Main.PRODUCTION)
                        .net(new Net("localhost", 12345, Network.localhostIsIPv6())).build());
                _mongod = _mongodExe.start();
                MONGOSERVER = new FlapDoodleEmbeddedMongo();
            }
        } catch (Exception e) {
            System.out.println(e);
            _mongod.stop();
            _mongodExe.stop();
            throw new RuntimeException("Could not create mongo client", e);
        }
        return MONGOSERVER;
    }

    public MongoClient client() {
        CodecRegistry registry = fromRegistries(CodecRegistries.fromCodecs(new BitemporalCodec()),
                MongoClientSettings.getDefaultCodecRegistry(),
                fromProviders(PojoCodecProvider.builder().automatic(true).build()));
        MongoClientSettings settings = MongoClientSettings.builder().codecRegistry(registry)
                .applyConnectionString(new ConnectionString("mongodb://localhost:12345")).build();
        MongoClient client = MongoClients.create(settings);
        return client;
    }

    public void testStartAndStopMongoImportAndMongod(String jsonFile, String database, String collection) throws UnknownHostException, IOException {
        int defaultConfigPort = 12345;
        String defaultHost = "localhost";

        MongoImportProcess mongoImport = startMongoImport(defaultHost, defaultConfigPort, database, collection,
                jsonFile, true, true, true);
        try {
            MongoClient mongoClient = client();
            System.out.println("DB Names: " + mongoClient.getDatabase(database).getName());
        } finally {
            mongoImport.stop();
        }
    }

    private MongoImportProcess startMongoImport(String bindIp, int port, String dbName, String collection,
            String jsonFile, Boolean jsonArray, Boolean upsert, Boolean drop) throws UnknownHostException, IOException {
        IMongoImportConfig mongoImportConfig = new MongoImportConfigBuilder().version(Version.Main.PRODUCTION)
                .net(new Net(bindIp, port, Network.localhostIsIPv6())).db(dbName).collection(collection).upsert(upsert)
                .dropCollection(drop).jsonArray(jsonArray).importFile(jsonFile).build();

        MongoImportExecutable mongoImportExecutable = MongoImportStarter.getDefaultInstance()
                .prepare(mongoImportConfig);
        MongoImportProcess mongoImport = mongoImportExecutable.start();
        return mongoImport;
    }

}
