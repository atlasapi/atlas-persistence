package org.atlasapi.persistence.content.elasticsearch.schema;

import com.google.common.base.Function;
import java.util.HashMap;
import java.util.Map;

/**
 */
public class ESObject {

    public final static FromEsObjectToMap TO_MAP = new FromEsObjectToMap();
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

    private static class FromEsObjectToMap implements Function<ESObject, Map> {

        @Override
        public Map apply(ESObject input) {
            return input.toMap();
        }
    }
}
