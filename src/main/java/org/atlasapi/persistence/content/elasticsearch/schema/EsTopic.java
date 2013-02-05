package org.atlasapi.persistence.content.elasticsearch.schema;

import org.atlasapi.media.entity.Publisher;

import com.google.common.collect.Iterables;


public class EsTopic extends EsObject {

    public static final String TYPE = "topic";

    private static final String ID = "aid";
    private static final String SOURCE = "source";
    private static final String ALIASES = "alias";
    private static final String TITLE = "title";
    private static final String DESCRIPTION = "description";

    
    public EsTopic id(long id) {
        properties.put(ID, id);
        return this;
    }
    
    public EsTopic source(Publisher source) {
        properties.put(SOURCE, source.key());
        return this;
    }
    
    public EsTopic aliases(Iterable<EsAlias> aliases) {
        properties.put(ALIASES, Iterables.transform(aliases, TO_MAP));
        return this;
    }
    
    public EsTopic title(String title) {
        properties.put(TITLE, title);
        return this;
    }
    
    public EsTopic description(String desc) {
        properties.put(DESCRIPTION, desc);
        return this;
    }
}
