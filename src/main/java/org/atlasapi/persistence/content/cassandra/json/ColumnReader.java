package org.atlasapi.persistence.content.cassandra.json;

/**
 */
public interface ColumnReader {
    
    byte[] read(String key);
}
