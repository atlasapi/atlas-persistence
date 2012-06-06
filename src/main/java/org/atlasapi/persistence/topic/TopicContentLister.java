package org.atlasapi.persistence.topic;

import org.atlasapi.content.criteria.ContentQuery;
import org.atlasapi.media.entity.Content;

public interface TopicContentLister {

    Iterable<Content> contentForTopic(Long topicId, ContentQuery contentQuery); 
    
}
