package org.atlasapi.persistence.topic;

import org.atlasapi.media.topic.Topic;

public interface TopicWriter {

    void write(Topic topic);

}
