package org.atlasapi.persistence.topic;

import java.util.UUID;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;

import org.atlasapi.media.entity.Topic;
import org.atlasapi.messaging.EntityUpdatedMessage;
import org.atlasapi.serialization.json.JsonFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.MessageCreator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.metabroadcast.common.time.Clock;
import com.metabroadcast.common.time.SystemClock;

public class MessageQueueingTopicWriter extends ForwardingTopicStore {

    private static final Logger log = LoggerFactory.getLogger(MessageQueueingTopicWriter.class);
    private final ObjectMapper mapper = JsonFactory.makeJsonMapper();
    private final JmsTemplate template;
    private final TopicStore delegate;
    private final Clock clock;

    public MessageQueueingTopicWriter(JmsTemplate template, TopicStore delegate) {
        this.template = template;
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
        template.send(new MessageCreator() {
            @Override
            public Message createMessage(Session session) throws JMSException {
                return session.createTextMessage(serialize(createEntityUpdatedMessage(topic)));
            }
        });
    }

    private EntityUpdatedMessage createEntityUpdatedMessage(Topic topic) {
        return new EntityUpdatedMessage(
                UUID.randomUUID().toString(),
                clock.now().getMillis(),
                topic.getId().toString(),
                topic.getClass().getSimpleName().toLowerCase(),
                topic.getPublisher().key());
    }

    private String serialize(final EntityUpdatedMessage message) {
        String result = null;
        try {
            result = mapper.writeValueAsString(message);
        } catch (Exception e) {
            log.error(message.getEntityId(), e);
        }
        return result;
    }
    
}
