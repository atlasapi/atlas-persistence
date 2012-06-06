package org.atlasapi.persistence.topic;

import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Topic;

import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.ids.IdGenerator;

public class TopicCreatingTopicResolver extends ForwardingTopicStore {

    private final IdGenerator idGenerator;
    private final TopicStore delegate;

    public TopicCreatingTopicResolver(TopicStore delegate, IdGenerator idGenerator) {
        this.delegate = delegate;
        this.idGenerator = idGenerator;
    }
    
    public Maybe<Topic> topicFor(String namespace, String value) {
        Maybe<Topic> topic = delegate().topicFor(namespace, value);
        return topicOrNewTopic(topic, null, namespace, value);
    }

    @Override
    protected TopicStore delegate() {
        return delegate;
    }

	@Override
	public Maybe<Topic> topicFor(Publisher publisher, String namespace,
			String value) {
		Maybe<Topic> topic = delegate().topicFor(publisher, namespace, value);
		return topicOrNewTopic(topic, publisher, namespace, value);
	}

	private Maybe<Topic> topicOrNewTopic(Maybe<Topic> topic,
			Publisher publisher, String namespace, String value) {
		if(topic.hasValue()) {
            return topic;
        } else {
            Topic newTopic = new Topic(idGenerator.generateRaw());
            newTopic.setNamespace(namespace);
            newTopic.setValue(value);
            newTopic.setPublisher(publisher);
            return Maybe.just(newTopic);
        }
	}
        
}
