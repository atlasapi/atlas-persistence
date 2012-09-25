package org.atlasapi.persistence.topic;

import org.atlasapi.media.entity.Topic;

/**
 */
public interface TopicLister {

    public Iterable<Topic> topics();
}
