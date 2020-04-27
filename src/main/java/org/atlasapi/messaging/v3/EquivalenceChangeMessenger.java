package org.atlasapi.messaging.v3;

import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Longs;
import com.metabroadcast.common.queue.MessageSender;
import com.metabroadcast.common.time.Timestamp;
import com.metabroadcast.common.time.Timestamper;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.persistence.lookup.entry.EquivRefs;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkNotNull;

public class EquivalenceChangeMessenger {

    private static final Logger log = LoggerFactory
            .getLogger(EquivalenceChangeMessenger.class);

    private final MessageSender<EquivalenceChangeMessage> sender;
    private final Timestamper timestamper;

    private EquivalenceChangeMessenger(
            MessageSender<EquivalenceChangeMessage> sender,
            Timestamper timestamper
    ) {
        this.sender = checkNotNull(sender);
        this.timestamper = checkNotNull(timestamper);
    }

    public static EquivalenceChangeMessenger create(
            MessageSender<EquivalenceChangeMessage> sender,
            Timestamper timestamper
    ) {
        return new EquivalenceChangeMessenger(sender, timestamper);
    }

    public void sendMessageFromDirectEquivs(
            LookupEntry existingSubjectEntry,
            LookupEntry newSubjectEntry,
            Set<String> sources
    ) {
        ImmutableSet.Builder<Long> added = ImmutableSet.builder();
        ImmutableSet.Builder<Long> removed = ImmutableSet.builder();
        ImmutableSet.Builder<Long> unchanged = ImmutableSet.builder();

        for (LookupRef ref : existingSubjectEntry.directEquivalents().getOutgoing()) {
            if (newSubjectEntry.directEquivalents().contains(ref, EquivRefs.Direction.OUTGOING)) {
                unchanged.add(ref.id());
            } else {
                if (!sources.contains(ref.publisher().key())) {
                    log.warn(
                            "Subject {} removed {} which did not belong to {}",
                            new Object[]{newSubjectEntry.uri(), ref.uri(), sources}
                    );
                }
                removed.add(ref.id());
            }
        }

        for (LookupRef ref : newSubjectEntry.directEquivalents().getOutgoing()) {
            if (!existingSubjectEntry.directEquivalents().contains(ref, EquivRefs.Direction.OUTGOING)) {
                added.add(ref.id());
                if (!sources.contains(ref.publisher().key())) {
                    log.warn(
                            "Subject {} added {} which did not belong to {}",
                            new Object[]{newSubjectEntry.uri(), ref.uri(), sources}
                    );
                }
            }
        }

        sendMessage(
                newSubjectEntry,
                added.build(),
                removed.build(),
                unchanged.build(),
                sources
        );
    }

    public void sendMessage(
            LookupEntry subject,
            Set<Long> outgoingIdsAdded,
            Set<Long> outgoingIdsRemoved,
            Set<Long> outgoingIdsUnchanged,
            Set<String> sources
    ) {
        try {
            String messageId = UUID.randomUUID().toString();
            Timestamp timestamp = timestamper.timestamp();
            EquivalenceChangeMessage message = new EquivalenceChangeMessage(
                    messageId,
                    timestamp,
                    subject.id(),
                    outgoingIdsAdded,
                    outgoingIdsRemoved,
                    outgoingIdsUnchanged,
                    sources
            );

            sender.sendMessage(
                    message,
                    getMessagePartitionKey(subject)
            );
        } catch (Exception e) {
            log.error("Failed to send equiv result message: " + subject, e);
        }
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
