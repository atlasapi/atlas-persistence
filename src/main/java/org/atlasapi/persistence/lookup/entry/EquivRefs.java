package org.atlasapi.persistence.lookup.entry;

import com.google.common.collect.ImmutableMap;
import com.metabroadcast.common.stream.MoreCollectors;
import org.atlasapi.media.entity.LookupRef;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Set;

//TODO: Unit tests
public class EquivRefs {

    public enum EquivDirection { // A lot of logic is heavily tied to these enum values
        INCOMING,
        OUTGOING,
        BIDIRECTIONAL,
        ;

        public boolean isIncoming() {
            return this == INCOMING || this == BIDIRECTIONAL;
        }

        public boolean isOnlyIncoming() {
            return this == INCOMING;
        }

        public boolean isOutgoing() {
            return this == OUTGOING || this == BIDIRECTIONAL;
        }

        public boolean isOnlyOutgoing() {
            return this == OUTGOING;
        }

        public boolean isBidirectional() {
            return this == BIDIRECTIONAL;
        }

        public boolean is(EquivDirection direction) {
            return this == BIDIRECTIONAL || this == direction;
        }

        public EquivDirection add(EquivDirection equivDirection) {
            if (this == equivDirection) {
                return this;
            }
            return BIDIRECTIONAL;
        }

        @Nullable
        public EquivDirection remove(EquivDirection equivDirection) {
            if (this == equivDirection || equivDirection == BIDIRECTIONAL) {
                return null;
            }

            if (this == BIDIRECTIONAL) {
                return equivDirection == INCOMING ? OUTGOING : INCOMING;
            }

            // this is not bidirectional and not the same as equivDirection so there's nothing to remove
            return this;
        }
    }

    private final Map<LookupRef, EquivDirection> equivRefs;

    private EquivRefs(Map<LookupRef, EquivDirection> equivRefs) {
        this.equivRefs = ImmutableMap.copyOf(equivRefs);
    }

    public static EquivRefs of(Map<LookupRef, EquivDirection> equivRefs) {
        return new EquivRefs(equivRefs);
    }

    public static EquivRefs of(LookupRef lookupRef, EquivDirection direction) {
        return new EquivRefs(ImmutableMap.of(lookupRef, direction));
    }

    public static EquivRefs of() {
        return new EquivRefs(ImmutableMap.of());
    }

    public Map<LookupRef, EquivDirection> getEquivRefs() {
        return equivRefs;
    }

    public Set<LookupRef> getLookupRefs() {
        return equivRefs.keySet();
    }

    /**
     * Returns whether there is any equiv link to or from the specified lookupRef.
     */
    public boolean contains(LookupRef lookupRef) {
        return equivRefs.containsKey(lookupRef);
    }

    /**
     * Returns whether there is the specified equiv link. If the link exists as a bidirectional link when a particular
     * single direction was specified then this will return true.
     */
    public boolean contains(LookupRef lookupRef, EquivDirection direction) {
        EquivDirection equivDirection = equivRefs.get(lookupRef);
        if (equivDirection == null) {
            return false;
        }
        return equivDirection.is(direction);
    }

    /**
     * Get all refs corresponding to outgoing or bidirectional equiv links
     */
    public Set<LookupRef> getOutgoing() {
        return equivRefs.entrySet().stream()
                .filter(entry -> entry.getValue().isOutgoing())
                .map(Map.Entry::getKey)
                .collect(MoreCollectors.toImmutableSet());
    }

    /**
     * Get all the refs corresponding to incoming or bidirectional equiv links
     */
    public Set<LookupRef> getIncoming() {
        return equivRefs.entrySet().stream()
                .filter(entry -> entry.getValue().isIncoming())
                .map(Map.Entry::getKey)
                .collect(MoreCollectors.toImmutableSet());
    }

    /**
     * Takes a copy and adds an equiv link. No copy is taken if the link was already present.
     */
    public EquivRefs copyWithLink(LookupRef lookupRef, EquivDirection direction) {
        EquivDirection existingDirection = equivRefs.get(lookupRef);
        if (existingDirection != null && existingDirection.is(direction)) {
            return this;
        }

        ImmutableMap.Builder<LookupRef, EquivDirection> newEquivs = ImmutableMap.builder();

        if (!equivRefs.containsKey(lookupRef)) {
            newEquivs.put(lookupRef, direction);
        }

        for (Map.Entry<LookupRef, EquivDirection> equivRef : equivRefs.entrySet()) {
            if (equivRef.getKey().equals(lookupRef)) {
                newEquivs.put(equivRef.getKey(), equivRef.getValue().add(direction));
            } else {
                newEquivs.put(equivRef);
            }
        }

        return new EquivRefs(newEquivs.build());
    }

    /**
     * Takes a copy and removes an equiv link. No copy is taken if the link was not present.
     */
    public EquivRefs copyWithoutLink(LookupRef lookupRef, EquivDirection direction) {
        EquivDirection existingDirection = equivRefs.get(lookupRef);
        if (existingDirection == null || !existingDirection.is(direction)) {
            return this;
        }

        ImmutableMap.Builder<LookupRef, EquivDirection> newEquivs = ImmutableMap.builder();
        for (Map.Entry<LookupRef, EquivDirection> equivRef : equivRefs.entrySet()) {
            if (equivRef.getKey().equals(lookupRef)) {
                EquivDirection newEquivDirection = equivRef.getValue().remove(direction);
                if (newEquivDirection == null) {
                    continue;
                }
                newEquivs.put(equivRef.getKey(), newEquivDirection);
            } else {
                newEquivs.put(equivRef);
            }
        }

        return new EquivRefs(newEquivs.build());
    }
}
