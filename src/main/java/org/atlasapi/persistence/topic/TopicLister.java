package org.atlasapi.persistence.topic;


import org.atlasapi.content.criteria.ContentQuery;
import org.atlasapi.media.entity.Topic;

public interface TopicLister {

    Iterable<Topic> topicsFor(ContentQuery query);
    
}
