package org.atlasapi.messaging.v3;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Longs;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.queue.MessageSender;
import com.metabroadcast.common.stream.MoreCollectors;
import com.metabroadcast.common.time.Timestamp;
import com.metabroadcast.common.time.Timestamper;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.persistence.content.KnownTypeContentResolver;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.google.common.base.Preconditions.checkNotNull;

public class ContentEquivalenceAssertionMessenger {

    private static final Logger log = LoggerFactory
            .getLogger(ContentEquivalenceAssertionMessenger.class);

    private final MessageSender<ContentEquivalenceAssertionMessage> sender;
    private final Timestamper timestamper;
    private final LookupEntryStore lookupEntryStore;
    private final KnownTypeContentResolver contentResolver;
    private final NumberToShortStringCodec entityIdCodec;

    private ContentEquivalenceAssertionMessenger(
            MessageSender<ContentEquivalenceAssertionMessage> sender,
            Timestamper timestamper,
            LookupEntryStore lookupEntryStore,
            KnownTypeContentResolver contentResolver
    ) {
        this.sender = checkNotNull(sender);
        this.timestamper = checkNotNull(timestamper);
        this.lookupEntryStore = checkNotNull(lookupEntryStore);
        this.contentResolver = checkNotNull(contentResolver);
        this.entityIdCodec = SubstitutionTableNumberCodec.lowerCaseOnly();
    }

    public static ContentEquivalenceAssertionMessenger create(
            MessageSender<ContentEquivalenceAssertionMessage> sender,
            Timestamper timestamper,
            LookupEntryStore lookupEntryStore,
            KnownTypeContentResolver contentResolver
    ) {
        return new ContentEquivalenceAssertionMessenger(sender, timestamper, lookupEntryStore, contentResolver);
    }

    public void sendMessage(
            LookupEntry subject,
            Collection<LookupRef> adjacents,
            Set<String> sources
    ) {
        try {
            Identified subjectContent = contentResolver.findByLookupRefs(ImmutableList.of(subject.lookupRef()))
                    .getFirstValue()
                    .requireValue();

            if (!(subjectContent instanceof Content)) {
                throw new IllegalArgumentException(subject + " is not a piece of content");
            }

            ImmutableList<Content> adjacentContent = contentResolver.findByLookupRefs(adjacents)
                    .getAllResolvedResults()
                    .stream()
                    .filter(Content.class::isInstance)
                    .map(Content.class::cast)
                    .collect(MoreCollectors.toImmutableList());

            if (adjacentContent.size() != adjacents.size()) {
                log.warn("Not all adjacents for {} were successfully resolved", subject);
            }

            ContentEquivalenceAssertionMessage message = messageFrom(
                    (Content) subjectContent,
                    adjacentContent,
                    ImmutableSet.copyOf(sources)
            );

            sender.sendMessage(
                    message,
                    getMessagePartitionKey(subject)
            );

        } catch (Exception e) {
            log.error("Failed to send equiv update message: " + subject, e);
        }
    }

    public void sendMessage(
            Content subject,
            ImmutableList<Content> adjacents,
            ImmutableSet<String> sources
    ) {
        try {
            ContentEquivalenceAssertionMessage message = messageFrom(
                    subject,
                    adjacents,
                    sources
            );

            sender.sendMessage(
                    message,
                    getMessagePartitionKey(subject)
            );
        } catch (Exception e) {
            log.error("Failed to send equiv update message: " + subject, e);
        }
    }

    private ContentEquivalenceAssertionMessage messageFrom(
            Content subject,
            ImmutableList<Content> adjacents,
            ImmutableSet<String> sources
    ) {
        String messageId = UUID.randomUUID().toString();
        Timestamp timestamp = timestamper.timestamp();

        String subjectId = entityIdCodec.encode(BigInteger.valueOf(subject.getId()));
        String subjectType = subject.getClass().getSimpleName().toLowerCase();
        String subjectSource = subject.getPublisher().key();

        ImmutableList<ContentEquivalenceAssertionMessage.AdjacentRef> adjacentRefs = adjacents(
                adjacents
        );

        return new ContentEquivalenceAssertionMessage(
                messageId,
                timestamp,
                subjectId,
                subjectType,
                subjectSource,
                adjacentRefs,
                sources
        );
    }

    private ImmutableList<ContentEquivalenceAssertionMessage.AdjacentRef> adjacents(
            ImmutableList<Content> adjacents
    ) {
        return adjacents.stream()
                .map(candidate -> new ContentEquivalenceAssertionMessage.AdjacentRef(
                        entityIdCodec.encode(BigInteger.valueOf(candidate.getId())),
                        candidate.getClass().getSimpleName().toLowerCase(),
                        candidate.getPublisher().key()
                ))
                .collect(MoreCollectors.toImmutableList());
    }

    private byte[] getMessagePartitionKey(Content subject) {
        Iterable<LookupEntry> lookupEntries = lookupEntryStore.entriesForIds(
                ImmutableSet.of(subject.getId())
        );

        Optional<LookupEntry> lookupEntryOptional = StreamSupport.stream(
                lookupEntries.spliterator(),
                false
        )
                .findFirst();

        if (lookupEntryOptional.isPresent()) {
            return getMessagePartitionKey(lookupEntryOptional.get());
        }

        // Default to returning the subject ID as the partition key
        return Longs.toByteArray(subject.getId());
    }

    private byte[] getMessagePartitionKey(LookupEntry lookupEntry) {

        // Given most of the time the equivalence results do not change the existing graph
        // (due to the fact that we are often rerunning equivalence on the same items with
        // the same results) the underlying graph will remain unchanged. Therefore if we get
        // the smallest lookup entry ID from that graph that ID should be consistent enough
        // to use as a partition key and ensure updates on the same graph end up on the same
        // partition.
        Optional<Long> graphId = Stream.concat(
                lookupEntry.equivalents().stream(),
                lookupEntry.getNeighbours().stream()
        )
                .map(LookupRef::id)
                .sorted()
                .findFirst();

        if (graphId.isPresent()) {
            return Longs.toByteArray(graphId.get());
        }

        // Default to returning the subject ID as the partition key
        return Longs.toByteArray(lookupEntry.id());
    }
}
