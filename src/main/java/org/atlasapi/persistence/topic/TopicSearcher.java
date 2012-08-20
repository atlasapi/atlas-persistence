package org.atlasapi.persistence.topic;

import java.util.List;
import java.util.Set;
import org.atlasapi.media.entity.Topic;
import org.joda.time.Interval;

/**
 */
public interface TopicSearcher {
   
    List<Topic> popularTopics(Interval interval, TopicQueryResolver resolver);
}
