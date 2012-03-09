package org.atlasapi.persistence.content.cassandra.json;

/**
 */
public interface ColumnReader {
    
    String read(String key);
}
