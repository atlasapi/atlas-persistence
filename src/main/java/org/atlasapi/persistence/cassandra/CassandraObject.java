package org.atlasapi.persistence.cassandra;

import com.google.common.base.Function;
import java.util.HashMap;
import java.util.Map;

/**
 */
public class CassandraObject {

    public final static FromCassandraObjectToMap TO_MAP = new FromCassandraObjectToMap();
    //
    protected Map properties = new HashMap() {

        @Override
        public Object put(Object k, Object v) {
            if (k != null && v != null) {
                return super.put(k, v);
            } else {
                return null;
            }
        }
    };

    public Map toMap() {
        return properties;
    }

    private static class FromCassandraObjectToMap implements Function<CassandraObject, Map> {

        @Override
        public Map apply(CassandraObject input) {
            return input.toMap();
        }
    }
}
