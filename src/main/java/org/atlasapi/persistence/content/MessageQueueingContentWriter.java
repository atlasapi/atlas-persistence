package org.atlasapi.persistence.content;

import java.util.UUID;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;

import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Item;
import org.atlasapi.messaging.v3.EntityUpdatedMessage;
import org.atlasapi.persistence.media.entity.ContainerTranslator;
import org.atlasapi.persistence.media.entity.ItemTranslator;
import org.atlasapi.serialization.json.JsonFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.MessageCreator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.time.Clock;
import com.metabroadcast.common.time.SystemClock;

public class MessageQueueingContentWriter implements ContentWriter {

    private static final Logger log = LoggerFactory.getLogger(MessageQueueingContentWriter.class);
    private final ObjectMapper mapper = JsonFactory.makeJsonMapper();
    private final JmsTemplate template;
    private final ContentWriter delegate;
    private final Clock clock;
    
    private final SubstitutionTableNumberCodec idCodec = new SubstitutionTableNumberCodec();
    private final ItemTranslator itemTranslator = new ItemTranslator(idCodec);
    private final ContainerTranslator containerTranslator = new ContainerTranslator(idCodec);

    public MessageQueueingContentWriter(JmsTemplate template, ContentWriter delegate) {
        this(template, delegate, new SystemClock());
    }

    public MessageQueueingContentWriter(JmsTemplate template, ContentWriter delegate, Clock clock) {
        this.template = template;
        this.delegate = delegate;
        this.clock = clock;
    }

    @Override
    public void createOrUpdate(Item item) {
        delegate.createOrUpdate(item);
        if (!item.hashChanged(itemTranslator.hashCodeOf(item))) {
            log.debug("{} not changed", item.getCanonicalUri());
            return;
        }
        enqueueMessageUpdatedMessage(item);
    }

    @Override
    public void createOrUpdate(Container container) {
        delegate.createOrUpdate(container);
        if (!container.hashChanged(containerTranslator.hashCodeOf(container))) {
            log.debug("{} un-changed", container.getCanonicalUri());
            return;
        }
        enqueueMessageUpdatedMessage(container);
    }

    private void enqueueMessageUpdatedMessage(final Content content) {
        template.send(new MessageCreator() {

            @Override
            public Message createMessage(Session session) throws JMSException {
                return session.createTextMessage(serialize(createEntityUpdatedMessage(content)));
            }
        });
    }

    private EntityUpdatedMessage createEntityUpdatedMessage(Content content) {
        return new EntityUpdatedMessage(
                UUID.randomUUID().toString(),
                clock.now().getMillis(),
                content.getCanonicalUri(),
                content.getClass().getSimpleName().toLowerCase(),
                content.getPublisher().key());
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