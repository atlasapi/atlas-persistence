package org.atlasapi.persistence.topic;

import java.util.List;

import org.atlasapi.media.topic.Topic;
import org.joda.time.Interval;

import com.metabroadcast.common.query.Selection;

public interface TopicSearcher {
   
    List<Topic> popularTopics(Interval interval, TopicQueryResolver resolver, Selection selection);
}
