package org.atlasapi.persistence.topic;

import java.util.UUID;

import org.atlasapi.media.entity.Topic;
import org.atlasapi.messaging.v3.EntityUpdatedMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.metabroadcast.common.queue.MessageSender;
import com.metabroadcast.common.time.Clock;
import com.metabroadcast.common.time.SystemClock;
import com.metabroadcast.common.time.Timestamper;

public class MessageQueueingTopicWriter extends ForwardingTopicStore {

    private static final Logger log = LoggerFactory.getLogger(MessageQueueingTopicWriter.class);
    private final MessageSender<EntityUpdatedMessage> sender;
    private final TopicStore delegate;
    private final Timestamper clock;

    public MessageQueueingTopicWriter(MessageSender<EntityUpdatedMessage> sender, TopicStore delegate) {
        this.sender = sender;
        this.delegate = delegate;
        this.clock = new SystemClock();
    }

    @Override
    protected TopicStore delegate() {
        return delegate;
    }
    
    @Override
    public void write(final Topic topic) {
        delegate.write(topic);
        try {
            sender.sendMessage(createEntityUpdatedMessage(topic));
        } catch (Exception e) {
            log.error("update message failed: " + topic, e);
        }
    }

    private EntityUpdatedMessage createEntityUpdatedMessage(Topic topic) {
        return new EntityUpdatedMessage(
                UUID.randomUUID().toString(),
                clock.timestamp(),
                topic.getId().toString(),
                topic.getClass().getSimpleName().toLowerCase(),
                topic.getPublisher().key());
    }
    
}
