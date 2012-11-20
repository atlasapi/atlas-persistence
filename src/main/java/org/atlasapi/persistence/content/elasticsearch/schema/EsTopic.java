package org.atlasapi.persistence.content.elasticsearch.schema;

/**
 */
public class EsTopic extends EsObject {

    public final static String ID = "id";

    public EsTopic id(Long id) {
        properties.put(ID, id);
        return this;
    }
}
