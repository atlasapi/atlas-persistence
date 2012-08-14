package org.atlasapi.persistence.topic;

import org.atlasapi.media.entity.Topic;

import com.metabroadcast.common.base.Maybe;
import org.atlasapi.content.criteria.ContentQuery;

public abstract class ForwardingTopicStore implements TopicStore {

    protected ForwardingTopicStore() {}
    
    protected abstract TopicStore delegate();
    
    @Override
    public Maybe<Topic> topicFor(String namespace, String value) {
        return delegate().topicFor(namespace, value);
    }
    
    @Override
    public void write(Topic topic) {
        delegate().write(topic);
    }

    public Iterable<Topic> topicsForIds(Iterable<Long> ids) {
        return delegate().topicsForIds(ids);
    }

    public Iterable<Topic> topicsFor(ContentQuery query) {
        return delegate().topicsFor(query);
    }

    public Maybe<Topic> topicForId(Long id) {
        return delegate().topicForId(id);
    }
    
}
