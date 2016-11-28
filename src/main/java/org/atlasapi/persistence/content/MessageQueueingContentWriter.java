package org.atlasapi.persistence.content;

import java.math.BigInteger;
import java.util.UUID;

import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Item;
import org.atlasapi.messaging.v3.ContentEquivalenceAssertionMessenger;
import org.atlasapi.messaging.v3.EntityUpdatedMessage;
import org.atlasapi.persistence.media.entity.ContainerTranslator;
import org.atlasapi.persistence.media.entity.ItemTranslator;

import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.queue.MessageSender;
import com.metabroadcast.common.stream.MoreCollectors;
import com.metabroadcast.common.time.SystemClock;
import com.metabroadcast.common.time.Timestamper;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Longs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class MessageQueueingContentWriter implements ContentWriter {

    private static final Logger log = LoggerFactory.getLogger(MessageQueueingContentWriter.class);
    private final MessageSender<EntityUpdatedMessage> sender;
    private final ContentWriter contentWriter;
    private final ContentResolver contentResolver;
    private final Timestamper clock;
    private final ContentEquivalenceAssertionMessenger messenger;

    private final SubstitutionTableNumberCodec idCodec = new SubstitutionTableNumberCodec();
    private final ItemTranslator itemTranslator = new ItemTranslator(idCodec);
    private final ContainerTranslator containerTranslator = new ContainerTranslator(idCodec);
    
    private final SubstitutionTableNumberCodec entityIdCodec =
            SubstitutionTableNumberCodec.lowerCaseOnly();

    public MessageQueueingContentWriter(
            ContentEquivalenceAssertionMessenger messenger,
            MessageSender<EntityUpdatedMessage> sender,
            ContentWriter contentWriter,
            ContentResolver contentResolver
    ) {
        this(messenger, sender, contentWriter, contentResolver, new SystemClock());
    }

    public MessageQueueingContentWriter(
            ContentEquivalenceAssertionMessenger messenger,
            MessageSender<EntityUpdatedMessage> sender,
            ContentWriter contentWriter,
            ContentResolver contentResolver,
            Timestamper clock
    ) {
        this.messenger = checkNotNull(messenger);
        this.sender = checkNotNull(sender);
        this.contentWriter = checkNotNull(contentWriter);
        this.contentResolver = checkNotNull(contentResolver);
        this.clock = checkNotNull(clock);
    }

    @Override
    public Item createOrUpdate(Item item) {
        Item writtenItem = contentWriter.createOrUpdate(item);
        if (!item.hashChanged(itemTranslator.hashCodeOf(item))) {
            log.debug("{} not changed", item.getCanonicalUri());
            return writtenItem;
        }
        enqueueMessageUpdatedMessage(item);
        return writtenItem;
    }

    @Override
    public void createOrUpdate(Container container) {
        contentWriter.createOrUpdate(container);
        if (!container.hashChanged(containerTranslator.hashCodeOf(container))) {
            log.debug("{} un-changed", container.getCanonicalUri());
            return;
        }
        enqueueMessageUpdatedMessage(container);
    }

    private void enqueueMessageUpdatedMessage(final Content content) {
        try {
            if(!content.getEquivalentTo().isEmpty()){
                ImmutableList<Content> adjacents = content.getEquivalentTo()
                        .stream()
                        .map(lookupRef -> contentResolver
                                .findByUris(ImmutableList.of(lookupRef.uri()))
                                .getFirstValue()
                        )
                        .filter(Maybe::hasValue)
                        .map(identifiedMaybe -> (Content) identifiedMaybe.requireValue())
                        .collect(MoreCollectors.toImmutableList());

                ImmutableSet<String> sources = content.getEquivalentTo()
                        .stream()
                        .map(lookupRef -> lookupRef.publisher().key())
                        .collect(MoreCollectors.toImmutableSet());

                messenger.sendMessage(
                        content,
                        adjacents,
                        sources
                );
            }
            sender.sendMessage(
                    createEntityUpdatedMessage(content), Longs.toByteArray(content.getId())
            );
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
