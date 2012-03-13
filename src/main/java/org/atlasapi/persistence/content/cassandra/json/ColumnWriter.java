package org.atlasapi.persistence.content.cassandra.json;

/**
 */
public interface ColumnWriter {
    
    void write(String key, byte[] value);
}
