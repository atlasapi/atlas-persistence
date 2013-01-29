package org.atlasapi.persistence.topic;

import org.atlasapi.media.topic.Topic;

@Deprecated
public interface TopicWriter {

    void write(Topic topic);

}
