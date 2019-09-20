package org.atlasapi.persistence.content;

import java.util.Set;

import javax.annotation.Nullable;

import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.messaging.v3.ContentEquivalenceAssertionMessenger;
import org.atlasapi.messaging.v3.EntityUpdatedMessage;

import com.metabroadcast.common.queue.MessageSender;
import com.metabroadcast.common.time.SystemClock;
import com.metabroadcast.common.time.Timestamper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * An extension of the MessageQueueingContentWriter which allows for writing empty equivalence sets
 * The constructor requires a different type signature for the ContentWriter which is why we created a sub-class
 */
public class MessageQueueingEquivalenceContentWriter extends MessageQueueingContentWriter implements EquivalenceContentWriter {

    private static final Logger log = LoggerFactory.getLogger(MessageQueueingEquivalenceContentWriter.class);
    private static final Logger timerLog = LoggerFactory.getLogger("TIMER");

    private final EquivalenceContentWriter equivalenceContentWriter;

    public MessageQueueingEquivalenceContentWriter(
            ContentEquivalenceAssertionMessenger messenger,
            MessageSender<EntityUpdatedMessage> sender,
            EquivalenceContentWriter equivalenceContentWriter,
            NullRemoveFieldsContentWriter nullRemoveFieldsContentWriter,
            ContentResolver contentResolver
    ) {
        this(
                messenger,
                sender,
                equivalenceContentWriter,
                nullRemoveFieldsContentWriter,
                contentResolver,
                new SystemClock()
        );
    }

    public MessageQueueingEquivalenceContentWriter(
            ContentEquivalenceAssertionMessenger messenger,
            MessageSender<EntityUpdatedMessage> sender,
            EquivalenceContentWriter equivalenceContentWriter,
            NullRemoveFieldsContentWriter nullRemoveFieldsContentWriter,
            ContentResolver contentResolver,
            Timestamper clock
    ) {
        super(
                messenger,
                sender,
                equivalenceContentWriter,
                nullRemoveFieldsContentWriter,
                contentResolver,
                clock
        );
        this.equivalenceContentWriter = checkNotNull(equivalenceContentWriter);
    }

    @Override
    public Item createOrUpdate(Item item, @Nullable Set<Publisher> publishers, boolean writeEquivalencesIfEmpty) {
        long lastTime = System.nanoTime();
        timerLog.debug("TIMER MQ entered. {} {}",item.getId(), Thread.currentThread().getName());
        Item writtenItem = equivalenceContentWriter.createOrUpdate(item, publishers, writeEquivalencesIfEmpty);
        timerLog.debug("TIMER MQ Delegate finished "+Long.toString((System.nanoTime() - lastTime)/1000000)+"ms. {} {}",item.getId(), Thread.currentThread().getName());
        lastTime = System.nanoTime();
        if (!item.hashChanged(itemTranslator.hashCodeOf(item))) {
            log.debug("{} not changed", item.getCanonicalUri());
            return writtenItem;
        }
        enqueueMessageUpdatedMessage(item, writeEquivalencesIfEmpty);

        timerLog.debug("TIMER MQ local work finished "+Long.toString((System.nanoTime() - lastTime)/1000000)+"ms. {} {}",item.getId(), Thread.currentThread().getName());
        return writtenItem;
    }

    @Override
    public void createOrUpdate(Container container, @Nullable Set<Publisher> publishers, boolean writeEquivalencesIfEmpty) {
        equivalenceContentWriter.createOrUpdate(container, publishers, writeEquivalencesIfEmpty);
        if (!container.hashChanged(containerTranslator.hashCodeOf(container))) {
            log.debug("{} un-changed", container.getCanonicalUri());
            return;
        }
        enqueueMessageUpdatedMessage(container, writeEquivalencesIfEmpty);
    }

}
