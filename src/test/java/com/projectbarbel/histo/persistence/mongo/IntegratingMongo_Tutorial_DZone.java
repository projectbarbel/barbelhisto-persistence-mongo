package com.projectbarbel.histo.persistence.mongo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.time.LocalDate;
import java.util.List;

import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.projectbarbel.histo.BarbelHisto;
import org.projectbarbel.histo.BarbelHistoBuilder;
import org.projectbarbel.histo.BarbelHistoContext;
import org.projectbarbel.histo.BarbelQueries;
import org.projectbarbel.histo.DocumentId;
import org.projectbarbel.histo.model.Bitemporal;

import com.googlecode.cqengine.query.QueryFactory;
import com.mongodb.client.MongoClient;

@TestMethodOrder(OrderAnnotation.class)
public class IntegratingMongo_Tutorial_DZone {

    @Order(1)
    @Test
    void dzoneTutorial_createInstance() throws Exception {
        MongoClient mongoClient = SimpleMongoListenerClient.createFromProperties().getMongoClient();
        mongoClient.getDatabase("testDb").drop();
        // update listener
        SimpleMongoUpdateListener updateListener = SimpleMongoUpdateListener.create(mongoClient, "testDb", "testCol",
                Client.class, BarbelHistoContext.getDefaultGson());
        // pre-fetch listener
        SimpleMongoLazyLoadingListener loadingListener = SimpleMongoLazyLoadingListener.create(mongoClient, "testDb",
                "testCol", Client.class, BarbelHistoContext.getDefaultGson());
        // locking listener
        MongoPessimisticLockingListener lockingListener = MongoPessimisticLockingListener.create(mongoClient, "lockDb",
                "docLocks");
        // BarbelHisto instance
        BarbelHisto<Client> mongoBackedHisto = BarbelHistoBuilder.barbel().withSynchronousEventListener(updateListener)
                .withSynchronousEventListener(loadingListener).withSynchronousEventListener(lockingListener).build();
        // save one
        Client client = new Client("1234", "Mr.", "Schlimm", "Niklas", "some street 11", "somemail@projectbarbel.org",
                LocalDate.of(1973, 6, 20));
        assertNotNull(mongoBackedHisto.save(client, LocalDate.now(), LocalDate.MAX));
    }

    @Order(2)
    @Test
    void dzoneTutorial_readOneLater() throws Exception {
        MongoClient mongoClient = SimpleMongoListenerClient.createFromProperties().getMongoClient();
        // update listener
        SimpleMongoUpdateListener updateListener = SimpleMongoUpdateListener.create(mongoClient, "testDb", "testCol",
                Client.class, BarbelHistoContext.getDefaultGson());
        // pre-fetch listener
        SimpleMongoLazyLoadingListener loadingListener = SimpleMongoLazyLoadingListener.create(mongoClient, "testDb",
                "testCol", Client.class, BarbelHistoContext.getDefaultGson());
        // locking listener
        MongoPessimisticLockingListener lockingListener = MongoPessimisticLockingListener.create(mongoClient, "lockDb",
                "docLocks");
        // BarbelHisto instance
        BarbelHisto<Client> mongoBackedHisto = BarbelHistoBuilder.barbel().withSynchronousEventListener(updateListener)
                .withSynchronousEventListener(loadingListener).withSynchronousEventListener(lockingListener).build();
        Client client = mongoBackedHisto.retrieveOne(BarbelQueries.effectiveNow("1234"));
        assertEquals("1234", client.getClientId());
        List<Client> clients = mongoBackedHisto.retrieve(QueryFactory.and(BarbelQueries.effectiveNow("1234"),BarbelQueries.effectiveNow("1234")));
        assertEquals(1, clients.size());
        Bitemporal clientBitemporal = (Bitemporal)client;
        System.out.println(clientBitemporal.getBitemporalStamp().toString());
        System.out.println(mongoBackedHisto.prettyPrintJournal("1234"));
    }
    
    public static class Client {

        @DocumentId
        private String clientId;
        private String title;

        private String name;
        private String firstname;
        private String address;
        private String email;
        private LocalDate dateOfBirth;

        public Client(String clientId, String title, String name, String firstname, String address, String email,
                LocalDate dateOfBirth) {
            super();
            this.clientId = clientId;
            this.title = title;
            this.name = name;
            this.firstname = firstname;
            this.address = address;
            this.email = email;
            this.dateOfBirth = dateOfBirth;
        }

        protected String getClientId() {
            return clientId;
        }

        protected void setClientId(String clientId) {
            this.clientId = clientId;
        }

        protected String getTitle() {
            return title;
        }

        protected void setTitle(String title) {
            this.title = title;
        }

        protected String getName() {
            return name;
        }

        protected void setName(String name) {
            this.name = name;
        }

        protected String getFirstname() {
            return firstname;
        }

        protected void setFirstname(String firstname) {
            this.firstname = firstname;
        }

        protected String getAddress() {
            return address;
        }

        protected void setAddress(String address) {
            this.address = address;
        }

        protected String getEmail() {
            return email;
        }

        protected void setEmail(String email) {
            this.email = email;
        }

        protected LocalDate getDateOfBirth() {
            return dateOfBirth;
        }

        protected void setDateOfBirth(LocalDate dateOfBirth) {
            this.dateOfBirth = dateOfBirth;
        }

    }

}
