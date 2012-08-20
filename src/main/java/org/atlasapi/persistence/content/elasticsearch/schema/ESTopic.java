package org.atlasapi.persistence.content.elasticsearch.schema;

/**
 */
public class ESTopic extends ESObject {

    public final static String ID = "id";

    public ESTopic id(Long id) {
        properties.put(ID, id);
        return this;
    }
}
