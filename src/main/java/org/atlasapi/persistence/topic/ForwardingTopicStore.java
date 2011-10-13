package org.atlasapi.persistence.topic;


import org.atlasapi.content.criteria.ContentQuery;
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
    public Maybe<Topic> topicForUri(String uri) {
        return delegate().topicForUri(uri);
    }

    @Override
    public void write(Topic topic) {
        delegate().write(topic);
    }

    @Override
    public Iterable<Topic> topicsForUris(Iterable<String> uris) {
        return delegate().topicsForUris(uris);
    }
    
    @Override
    public Iterable<Topic> topicsFor(ContentQuery query) {
        return delegate().topicsFor(query);
    }
    
}
