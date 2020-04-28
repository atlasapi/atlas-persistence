package org.atlasapi.messaging.v3;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.metabroadcast.common.queue.AbstractMessage;
import com.metabroadcast.common.time.Timestamp;

import java.util.Set;

/**
 * <p>
 * Message listing the change in equivalence on a particular subject for specified sources.
 * </p>
 */
public class EquivalenceChangeMessage extends AbstractMessage {

    private final long subjectId;
    private Set<Long> outgoingIdsAdded;
    private Set<Long> outgoingIdsRemoved;
    private Set<Long> outgoingIdsUnchanged;
    private Set<String> sources;

    /**
     * Creates a new {@link EquivalenceChangeMessage}.
     *
     * @param messageId
     *            - a unique identifier for this message.
     * @param timestamp
     *            - the time this message was created.
     * @param subjectId
     *            - the id of the <i>subject</i> of this message.
     * @param outgoingIdsAdded
     *            - set of ids added as outgoing links from the <i>subject</i>.
     * @param outgoingIdsRemoved
     *            - set of ids removed as outgoing links from the <i>subject</i>.
     * @param outgoingIdsUnchanged
     *            - set of ids which existed as outgoing links from the <i>subject</i> and were unchanged.
     * @param sources
     *            - set of keys of sources for which these changes relate to.
     */
    public EquivalenceChangeMessage(
            String messageId,
            Timestamp timestamp,
            long subjectId,
            Set<Long> outgoingIdsAdded,
            Set<Long> outgoingIdsRemoved,
            Set<Long> outgoingIdsUnchanged,
            Set<String> sources
    ) {
        super(messageId, timestamp);
        this.subjectId = subjectId;
        this.outgoingIdsAdded = outgoingIdsAdded == null
                ? ImmutableSet.of()
                : ImmutableSet.copyOf(outgoingIdsAdded);
        this.outgoingIdsRemoved = outgoingIdsRemoved == null
                ? ImmutableSet.of()
                : ImmutableSet.copyOf(outgoingIdsRemoved);
        this.outgoingIdsUnchanged = outgoingIdsUnchanged == null
                ? ImmutableSet.of()
                : ImmutableSet.copyOf(outgoingIdsUnchanged);
        this.sources = sources == null
                ? ImmutableSet.of()
                : ImmutableSet.copyOf(sources);
    }

    public long getSubjectId() {
        return subjectId;
    }

    public Set<Long> getOutgoingIdsAdded() {
        return outgoingIdsAdded;
    }

    public Set<Long> getOutgoingIdsRemoved() {
        return outgoingIdsRemoved;
    }

    public Set<Long> getOutgoingIdsUnchanged() {
        return outgoingIdsUnchanged;
    }

    @JsonIgnore
    public Set<Long> getOutgoingIdsChanged() {
        return Sets.union(outgoingIdsAdded, outgoingIdsRemoved);
    }

    @JsonIgnore
    public Set<Long> getOutgoingIds() {
        return Sets.union(outgoingIdsUnchanged, getOutgoingIdsChanged());
    }

    public Set<String> getSources() {
        return sources;
    }

}
