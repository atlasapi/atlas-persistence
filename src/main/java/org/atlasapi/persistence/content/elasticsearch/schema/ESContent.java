package org.atlasapi.persistence.content.elasticsearch.schema;

import com.google.common.collect.Iterables;
import java.util.Collection;
import java.util.Map;

/**
 */
public class ESContent extends ESObject {

    public final static String TOP_LEVEL_TYPE = "container";
    public final static String CHILD_TYPE = "child_item";
    public final static String TOP_ITEM_TYPE = "top_item";
    //
    public final static String URI = "uri";
    public final static String TITLE = "title";
    public final static String FLATTENED_TITLE = "flattenedTitle";
    public final static String PARENT_TITLE = "parentTitle";
    public final static String PARENT_FLATTENED_TITLE = "parentFlattenedTitle";
    public final static String PUBLISHER = "publisher";
    public final static String SPECIALIZATION = "specialization";
    public final static String BROADCASTS = "broadcasts";
    public final static String LOCATIONS = "locations";
    public final static String TOPICS = "topics";
    public final static String HAS_CHILDREN = "hasChildren";

    public ESContent uri(String uri) {
        properties.put(URI, uri);
        return this;
    }

    public ESContent title(String title) {
        properties.put(TITLE, title);
        return this;
    }
    
    public ESContent flattenedTitle(String flattenedTitle) {
        properties.put(FLATTENED_TITLE, flattenedTitle);
        return this;
    }

    public ESContent parentTitle(String parentTitle) {
        properties.put(PARENT_TITLE, parentTitle);
        return this;
    }
    
    public ESContent parentFlattenedTitle(String parentFlattenedTitle) {
        properties.put(PARENT_FLATTENED_TITLE, parentFlattenedTitle);
        return this;
    }
    
    public ESContent publisher(String publisher) {
        properties.put(PUBLISHER, publisher);
        return this;
    }

    public ESContent specialization(String specialization) {
        properties.put(SPECIALIZATION, specialization);
        return this;
    }

    public ESContent broadcasts(Collection<ESBroadcast> broadcasts) {
        properties.put(BROADCASTS, Iterables.transform(broadcasts, TO_MAP));
        return this;
    }

    public ESContent locations(Collection<ESLocation> locations) {
        properties.put(LOCATIONS, Iterables.transform(locations, TO_MAP));
        return this;
    }

    public ESContent topics(Collection<ESTopic> topics) {
        properties.put(TOPICS, Iterables.transform(topics, TO_MAP));
        return this;
    }
    
    public ESContent hasChildren(Boolean hasChildren) {
        properties.put(HAS_CHILDREN, hasChildren);
        return this;
    }
}
