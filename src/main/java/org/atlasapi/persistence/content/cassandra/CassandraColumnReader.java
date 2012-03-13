package org.atlasapi.persistence.content.cassandra;

import com.netflix.astyanax.model.ColumnList;
import org.atlasapi.persistence.content.cassandra.json.ColumnReader;

/**
 */
public class CassandraColumnReader implements ColumnReader {

    private final ColumnList<String> columns;

    public CassandraColumnReader(ColumnList<String> columns) {
        this.columns = columns;
    }
    
    @Override
    public byte[] read(String key) {
        return columns.getColumnByName(key).getByteArrayValue();
    }
    
}
