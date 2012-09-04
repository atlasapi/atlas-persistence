package org.atlasapi.persistence.topic;

import com.metabroadcast.common.query.Selection;
import java.util.List;
import org.atlasapi.media.entity.Topic;
import org.joda.time.Interval;

/**
 */
public interface TopicSearcher {
   
    List<Topic> popularTopics(Interval interval, TopicQueryResolver resolver, Selection selection);
}
