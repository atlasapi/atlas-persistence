package org.atlasapi.persistence.topic;


import org.atlasapi.content.criteria.ContentQuery;
import org.atlasapi.media.entity.Topic;

import com.google.common.collect.ImmutableList;
import com.metabroadcast.common.base.Maybe;

public interface TopicQueryResolver {

    Maybe<Topic> topicForId(Long id);

    Iterable<Topic> topicsForUris(Iterable<String> uris);
    
    Iterable<Topic> topicsFor(ContentQuery query);
    
    public static final TopicQueryResolver NULL_RESOLVER = new TopicQueryResolver() {
        
        @Override
        public Iterable<Topic> topicsForUris(Iterable<String> uris) {
            return ImmutableList.of();
        }
        
        @Override
        public Maybe<Topic> topicForId(Long id) {
            return Maybe.nothing();
        }

        @Override
        public Iterable<Topic> topicsFor(ContentQuery query) {
            return ImmutableList.of();
        }
    };
    
}
