package org.atlasapi.persistence.topic;


import org.atlasapi.media.entity.Topic;

import com.google.common.collect.ImmutableList;
import com.metabroadcast.common.base.Maybe;

public interface TopicUriResolver {

    Maybe<Topic> topicForUri(String uri);

    Iterable<Topic> topicsForUris(Iterable<String> uris);
    
    public static final TopicUriResolver NULL_RESOLVER = new TopicUriResolver() {
        
        @Override
        public Iterable<Topic> topicsForUris(Iterable<String> uris) {
            return ImmutableList.of();
        }
        
        @Override
        public Maybe<Topic> topicForUri(String uri) {
            return Maybe.nothing();
        }
    };
    
}
