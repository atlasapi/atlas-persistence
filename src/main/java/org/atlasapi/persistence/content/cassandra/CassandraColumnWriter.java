package org.atlasapi.persistence.content.cassandra;

import com.netflix.astyanax.ColumnListMutation;
import org.atlasapi.persistence.content.cassandra.json.ColumnWriter;

/**
 */
public class CassandraColumnWriter implements ColumnWriter {

    private final ColumnListMutation mutation;

    public CassandraColumnWriter(ColumnListMutation mutation) {
        this.mutation = mutation;
    }

    @Override
    public void write(String key, byte[] value) {
        this.mutation.putColumn(key, value, null);
    }
}
