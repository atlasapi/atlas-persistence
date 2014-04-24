package org.atlasapi.messaging.v3;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;
import java.util.Set;

import com.google.common.base.Objects;
import com.metabroadcast.common.time.Timestamp;

/**
 * <p>
 * Message asserting a direct equivalence between a <i>subject</i> and a number
 * of <i>adjacent</i> resources.
 * </p>
 * 
 * <p>
 * For example, a subject, α, is a neighbour of resources β and ɣ.
 * <p>
 * 
 * <p>
 * Also included is a set of source identifiers for which this assertion is
 * valid. The subject and adjacent resources should be from sources which are
 * members of this set.
 * </p>
 * 
 */
public class ContentEquivalenceAssertionMessage extends AbstractMessage {

    public static class AdjacentRef {
        
        private final String id;
        private final String type;
        private final String source;
        
        public AdjacentRef(String id, String type, String source) {
            this.id = checkNotNull(id);
            this.type = checkNotNull(type);
            this.source = checkNotNull(source);
        }
        
        public String getId() {
            return id;
        }
        
        public String getType() {
            return type;
        }
        
        public String getSource() {
            return source;
        }

        @Override
        public boolean equals(Object that) {
            if (this == that) {
                return true;
            }
            if (that instanceof AdjacentRef) {
                AdjacentRef other = (AdjacentRef) that;
                return id.equals(other.id)
                    && type.equals(other.type)
                    && source.equals(other.source);
            }
            return false;
        }
        
        @Override
        public int hashCode() {
            return Objects.hashCode(id, type, source);
        }
        
        @Override
        public String toString() {
            return String.format("%s(%s,%s)", id, type, source);
        }
        
    }
    
    private List<AdjacentRef> adjacent;
    private Set<String> sources;

    /**
     * Creates a new EquivalenceAssertionMessage.
     * 
     * @param messageId
     *            - a unique identifier for this message.
     * @param timestamp
     *            - the time this message was created.
     * @param subjectId
     *            - the id of the <i>subject</i> of this message.
     * @param subjectType
     *            - the type of the message the <i>subject</i> of this message.
     * @param subjectSource
     *            - the key of the source of the <i>subject</i> of the message.
     * @param adjacent
     *            - list of refs of adjacent resources.
     * @param sources
     *            - set of keys of sources for which this assertion is valid.
     */
    public ContentEquivalenceAssertionMessage(String messageId, Timestamp timestamp, String subjectId,
            String subjectType, String subjectSource, List<AdjacentRef> adjacent, Set<String> sources) {
        super(messageId, timestamp, subjectId, subjectType, subjectSource);
        this.adjacent = adjacent;
        this.sources = sources;
    }

    public List<AdjacentRef> getAdjacent() {
        return adjacent;
    }

    public Set<String> getSources() {
        return sources;
    }

}
