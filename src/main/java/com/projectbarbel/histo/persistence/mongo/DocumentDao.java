package com.projectbarbel.histo.persistence.mongo;

import java.util.Optional;

/**
 * Generic DAO Interface for bi-temporal data storage.
 * 
 * @author Niklas Schlimm
 *
 * @param <T> the application's document type to store with a concrete DAO
 *        implementation
 * @param <O> the object id type that is used by the application as unique
 *        object identifier
 */
public interface DocumentDao<T, O> {

    Optional<O> createDocument(T document);

    Optional<O> updateDocument(O objectId, T document);

    long deleteDocument(O objectId);

    Optional<T> readDocument(O objectId);

    void reset();

}
