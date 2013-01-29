package org.atlasapi.persistence.topic;

import org.atlasapi.media.topic.Topic;

@Deprecated
public interface TopicLister {

    public Iterable<Topic> topics();
}
