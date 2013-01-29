package org.atlasapi.persistence.topic;

import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.topic.Topic;

import com.metabroadcast.common.base.Maybe;

@Deprecated
public interface TopicLookupResolver {

    Maybe<Topic> topicFor(String namespace, String value);

    Maybe<Topic> topicFor(Publisher publisher, String namespace, String value);
}
