package com.projectbarbel.histo.persistence.impl.mongo;

import java.io.IOException;
import java.net.UnknownHostException;

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

    private static MongodStarter starter;
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
            starter=MongodStarter.getDefaultInstance();
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

    public void testStartAndStopMongoImportAndMongod(String jsonFile, String database, String collection)
            throws UnknownHostException, IOException {
        int defaultConfigPort = 12345;
        String defaultHost = "localhost";

        startMongoImport(defaultHost, defaultConfigPort, database, collection, jsonFile, true, true, true);
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
