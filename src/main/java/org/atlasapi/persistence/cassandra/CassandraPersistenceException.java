package org.atlasapi.persistence.cassandra;

/**
 */
public class CassandraPersistenceException extends RuntimeException {

    public CassandraPersistenceException(String message, Throwable cause) {
        super(message, cause);
    }
}
