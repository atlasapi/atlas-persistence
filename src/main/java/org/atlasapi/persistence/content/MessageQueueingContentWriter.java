package org.atlasapi.persistence.content;

import static com.google.common.base.Preconditions.checkNotNull;

import java.math.BigInteger;
import java.util.UUID;

import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Item;
import org.atlasapi.messaging.v3.EntityUpdatedMessage;
import org.atlasapi.persistence.media.entity.ContainerTranslator;
import org.atlasapi.persistence.media.entity.ItemTranslator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.queue.MessageSender;
import com.metabroadcast.common.time.SystemClock;
import com.metabroadcast.common.time.Timestamper;

public class MessageQueueingContentWriter implements ContentWriter {

    private static final Logger log = LoggerFactory.getLogger(MessageQueueingContentWriter.class);
    private final MessageSender<EntityUpdatedMessage> sender;
    private final ContentWriter delegate;
    private final Timestamper clock;
    
    private final SubstitutionTableNumberCodec idCodec = new SubstitutionTableNumberCodec();
    private final ItemTranslator itemTranslator = new ItemTranslator(idCodec);
    private final ContainerTranslator containerTranslator = new ContainerTranslator(idCodec);
    
    private final SubstitutionTableNumberCodec entityIdCodec = SubstitutionTableNumberCodec.lowerCaseOnly();

    public MessageQueueingContentWriter(MessageSender<EntityUpdatedMessage> sender, ContentWriter delegate) {
        this(sender, delegate, new SystemClock());
    }

    public MessageQueueingContentWriter(MessageSender<EntityUpdatedMessage> sender, ContentWriter delegate, Timestamper clock) {
        this.sender = checkNotNull(sender);
        this.delegate = checkNotNull(delegate);
        this.clock = checkNotNull(clock);
    }

    @Override
    public Item createOrUpdate(Item item) {
        Item writtenItem = delegate.createOrUpdate(item);
        if (!item.hashChanged(itemTranslator.hashCodeOf(item))) {
            log.debug("{} not changed", item.getCanonicalUri());
            return writtenItem;
        }
        enqueueMessageUpdatedMessage(item);
        return writtenItem;
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
        try {
            sender.sendMessage(createEntityUpdatedMessage(content));
        } catch (Exception e) {
            log.error("update message failed: " + content, e);
        }
    }

    private EntityUpdatedMessage createEntityUpdatedMessage(Content content) {
        return new EntityUpdatedMessage(
                UUID.randomUUID().toString(),
                clock.timestamp(),
                entityIdCodec.encode(BigInteger.valueOf(content.getId())),
                content.getClass().getSimpleName().toLowerCase(),
                content.getPublisher().key());
    }
}