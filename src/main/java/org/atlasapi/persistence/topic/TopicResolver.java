package org.atlasapi.persistence.topic;

import org.atlasapi.media.entity.Topic;

import com.metabroadcast.common.base.Maybe;

public interface TopicResolver {
    
    Maybe<Topic> topicFor(String namespace, String value);
    
}
