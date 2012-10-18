package org.atlasapi.persistence.elasticsearch;

/**
 */
public class ESPersistenceException extends RuntimeException {

    public ESPersistenceException(String message, Throwable cause) {
        super(message, cause);
    }
    
    public ESPersistenceException(String message) {
        super(message);
    }
}
