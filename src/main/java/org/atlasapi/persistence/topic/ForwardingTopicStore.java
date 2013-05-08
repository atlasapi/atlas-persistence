package org.atlasapi.persistence.topic;

import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Topic;

import com.metabroadcast.common.base.Maybe;

public abstract class ForwardingTopicStore implements TopicStore {

    protected ForwardingTopicStore() {}
    
    protected abstract TopicStore delegate();
    
    @Override
    public Maybe<Topic> topicFor(String namespace, String value) {
        return delegate().topicFor(namespace, value);
    }
    
    @Override
    public Maybe<Topic> topicFor(Publisher publisher, String namespace, String value) {
        return delegate().topicFor(publisher, namespace, value);
    }
    
    @Override
    public void write(Topic topic) {
        delegate().write(topic);
    }
    
}
