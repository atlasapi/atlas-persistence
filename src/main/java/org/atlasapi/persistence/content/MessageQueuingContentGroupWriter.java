package org.atlasapi.persistence.content;

import static com.google.common.base.Preconditions.checkNotNull;

import java.math.BigInteger;
import java.util.UUID;

import org.atlasapi.media.entity.ContentGroup;
import org.atlasapi.messaging.v3.EntityUpdatedMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.primitives.Longs;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.queue.MessageSender;
import com.metabroadcast.common.queue.MessagingException;
import com.metabroadcast.common.time.Timestamper;

public class MessageQueuingContentGroupWriter implements ContentGroupWriter {

    private static final Logger log = LoggerFactory.getLogger(MessageQueuingContentGroupWriter.class);

    private final SubstitutionTableNumberCodec entityIdCodec = SubstitutionTableNumberCodec.lowerCaseOnly();

    private final ContentGroupWriter delegate;
    private final MessageSender<EntityUpdatedMessage> messageSender;
    private final Timestamper clock;

    public MessageQueuingContentGroupWriter(ContentGroupWriter delegate,
            MessageSender<EntityUpdatedMessage> messageSender, Timestamper clock) {
        this.delegate = checkNotNull(delegate);
        this.messageSender = checkNotNull(messageSender);
        this.clock = checkNotNull(clock);
    }

    @Override
    public void createOrUpdate(ContentGroup group) {
        try {
            delegate.createOrUpdate(group);
            messageSender.sendMessage(
                    createEntityUpdatedMessage(group), Longs.toByteArray(group.getId())
            );
        } catch (MessagingException e) {
            log.error("update message failed: " + group.toString(), e);
        }
    }

    private EntityUpdatedMessage createEntityUpdatedMessage(ContentGroup contentGroup) {
        return new EntityUpdatedMessage(
                UUID.randomUUID().toString(),
                clock.timestamp(),
                entityIdCodec.encode(BigInteger.valueOf(contentGroup.getId())),
                contentGroup.getClass().getSimpleName().toLowerCase(),
                contentGroup.getPublisher().key()
        );
    }
}
