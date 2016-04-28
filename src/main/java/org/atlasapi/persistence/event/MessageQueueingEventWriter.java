package org.atlasapi.persistence.event;

import java.math.BigInteger;
import java.util.UUID;

import org.atlasapi.media.entity.Event;
import org.atlasapi.messaging.v3.EntityUpdatedMessage;

import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.queue.MessageSender;
import com.metabroadcast.common.time.SystemClock;
import com.metabroadcast.common.time.Timestamper;

import com.google.common.primitives.Longs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class MessageQueueingEventWriter implements EventWriter {

    private static final Logger LOG = LoggerFactory.getLogger(MessageQueueingEventWriter.class);

    private final EventWriter delegate;
    private final MessageSender<EntityUpdatedMessage> sender;
    private final Timestamper timestamper;
    private final SubstitutionTableNumberCodec entityIdCodec;

    public MessageQueueingEventWriter(EventWriter delegate,
            MessageSender<EntityUpdatedMessage> sender) {
        this(delegate, sender, new SystemClock(), SubstitutionTableNumberCodec.lowerCaseOnly());
    }

    public MessageQueueingEventWriter(EventWriter delegate, MessageSender<EntityUpdatedMessage> sender,
            Timestamper timestamper, SubstitutionTableNumberCodec entityIdCodec) {
        this.delegate = checkNotNull(delegate);
        this.sender = checkNotNull(sender);
        this.timestamper = checkNotNull(timestamper);
        this.entityIdCodec = checkNotNull(entityIdCodec);
    }

    @Override
    public Event createOrUpdate(Event event) {
        delegate.createOrUpdate(event);

        if(event.getId() != null) {
            enqueueMessageUpdatedEvent(event);
        }
        else {
            LOG.warn("Update message failed. Missing id for: " + event);
        }
        return event;
    }

    private void enqueueMessageUpdatedEvent(Event event) {
        try {
            sender.sendMessage(createEntityUpdatedMessage(event), Longs.toByteArray(event.getId()));
        } catch (Exception e) {
            LOG.warn("Update message failed: " + event, e);
        }
    }

    private EntityUpdatedMessage createEntityUpdatedMessage(Event event) {
        return new EntityUpdatedMessage(
                UUID.randomUUID().toString(),
                timestamper.timestamp(),
                entityIdCodec.encode(BigInteger.valueOf(event.getId())),
                event.getClass().getSimpleName().toLowerCase(),
                event.publisher().key()
        );
    }
}
