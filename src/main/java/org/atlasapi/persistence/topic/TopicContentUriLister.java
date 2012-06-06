package org.atlasapi.persistence.topic;

import org.atlasapi.content.criteria.ContentQuery;

public interface TopicContentUriLister {

    Iterable<String> contentUrisForTopic(Long topicId, ContentQuery contentQuery);
    
}
