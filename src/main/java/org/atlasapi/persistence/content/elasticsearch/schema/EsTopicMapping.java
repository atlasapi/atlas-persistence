package org.atlasapi.persistence.content.elasticsearch.schema;

/**
 */
public class EsTopicMapping extends EsObject {

    public final static String ID = "id";

    public EsTopicMapping id(Long id) {
        properties.put(ID, id);
        return this;
    }
}
