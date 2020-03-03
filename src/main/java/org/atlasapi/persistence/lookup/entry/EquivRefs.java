package org.atlasapi.persistence.lookup.entry;

import com.google.common.collect.ImmutableMap;
import com.metabroadcast.common.stream.MoreCollectors;
import org.atlasapi.media.entity.LookupRef;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Set;

import static org.atlasapi.persistence.lookup.entry.EquivRefs.EquivDirection.INCOMING;
import static org.atlasapi.persistence.lookup.entry.EquivRefs.EquivDirection.OUTGOING;

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

    public static EquivRefs of(Set<LookupRef> lookupRefs, EquivDirection direction) {
        return new EquivRefs(toMap(lookupRefs, direction));
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
     * Takes a copy and adds an equiv link.
     */
    public EquivRefs copyWithLink(LookupRef lookupRef, EquivDirection direction) {
        return copyWithLinks(toMap(lookupRef, direction));
    }

    /**
     * Takes a copy and adds the specified equiv links.
     */
    public EquivRefs copyWithLinks(Set<LookupRef> lookupRefs, EquivDirection direction) {
        return copyWithLinks(toMap(lookupRefs, direction));
    }

    /**
     * Takes a copy and adds the specified equiv links.
     */
    public EquivRefs copyWithLinks(Map<LookupRef, EquivDirection> equivRefsToAdd) {
        ImmutableMap.Builder<LookupRef, EquivDirection> newEquivs = ImmutableMap.builder();

        for (Map.Entry<LookupRef, EquivDirection> equivRef : equivRefsToAdd.entrySet()) {

            EquivDirection existingDirection = equivRefs.get(equivRef.getKey());

            if (existingDirection == null) {
                newEquivs.put(equivRef);
            } else {
                newEquivs.put(equivRef.getKey(), existingDirection.add(equivRef.getValue()));
            }
        }

        for (Map.Entry<LookupRef, EquivDirection> equivRef : equivRefs.entrySet()) {
            if (!equivRefsToAdd.containsKey(equivRef.getKey())) {
                newEquivs.put(equivRef);
            }
        }

        return new EquivRefs(newEquivs.build());
    }

    /**
     * Takes a copy and removes an equiv link.
     */
    public EquivRefs copyWithoutLink(LookupRef lookupRef, EquivDirection direction) {
        return copyWithoutLinks(toMap(lookupRef, direction));
    }

    /**
     * Takes a copy and removes the specified equiv links.
     */
    public EquivRefs copyWithoutLinks(Set<LookupRef> lookupRefs, EquivDirection direction) {
        return copyWithoutLinks(toMap(lookupRefs, direction));
    }

    /**
     * Takes a copy and removes the specified equiv links.
     */
    public EquivRefs copyWithoutLinks(Map<LookupRef, EquivDirection> equivRefsToRemove) {
        ImmutableMap.Builder<LookupRef, EquivDirection> newEquivs = ImmutableMap.builder();

        for (Map.Entry<LookupRef, EquivDirection> equivRef : equivRefsToRemove.entrySet()) {

            EquivDirection newEquivDirection = equivRefs.get(equivRef.getKey());

            if (newEquivDirection != null) {
                EquivDirection newDirection = newEquivDirection.remove(equivRef.getValue());
                if (newDirection != null) {
                    newEquivs.put(equivRef.getKey(), newDirection);
                }
            }
        }

        for (Map.Entry<LookupRef, EquivDirection> equivRef : equivRefs.entrySet()) {
            if (!equivRefsToRemove.containsKey(equivRef.getKey())) {
                newEquivs.put(equivRef);
            }
        }

        return new EquivRefs(newEquivs.build());
    }

    public EquivRefs copyAndReplaceOutgoing(Set<LookupRef> newOutgoing) {
        ImmutableMap.Builder<LookupRef, EquivDirection> newEquivs = ImmutableMap.builder();

        for (LookupRef lookupRef : newOutgoing) {
            EquivDirection existingDirection = equivRefs.get(lookupRef);

            if (existingDirection == null) {
                newEquivs.put(lookupRef, OUTGOING);
            } else {
                newEquivs.put(lookupRef, existingDirection.add(OUTGOING));
            }
        }

        for (Map.Entry<LookupRef, EquivDirection> equivRef : equivRefs.entrySet()) {
            if (!newOutgoing.contains(equivRef.getKey()) && equivRef.getValue().isIncoming()) {
                newEquivs.put(equivRef.getKey(), INCOMING);
            }
        }

        return new EquivRefs(newEquivs.build());
    }

    public EquivRefs copyAndReplaceIncoming(Set<LookupRef> newIncoming) {
        ImmutableMap.Builder<LookupRef, EquivDirection> newEquivs = ImmutableMap.builder();

        for (LookupRef lookupRef : newIncoming) {
            EquivDirection existingDirection = equivRefs.get(lookupRef);

            if (existingDirection == null) {
                newEquivs.put(lookupRef, INCOMING);
            } else {
                newEquivs.put(lookupRef, existingDirection.add(INCOMING));
            }
        }

        for (Map.Entry<LookupRef, EquivDirection> equivRef : equivRefs.entrySet()) {
            if (!newIncoming.contains(equivRef.getKey()) && equivRef.getValue().isOutgoing()) {
                newEquivs.put(equivRef.getKey(), OUTGOING);
            }
        }

        return new EquivRefs(newEquivs.build());
    }

    private static Map<LookupRef, EquivDirection> toMap(LookupRef lookupRef, EquivDirection direction) {
        return ImmutableMap.of(lookupRef, direction);
    }

    private static Map<LookupRef, EquivDirection> toMap(Set<LookupRef> lookupRefs, EquivDirection direction) {
        return lookupRefs.stream()
                .collect(MoreCollectors.toImmutableMap(ref -> ref, ref -> direction));
    }
}
