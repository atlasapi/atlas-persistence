package org.atlasapi.persistence.lookup.entry;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.metabroadcast.common.stream.MoreCollectors;
import com.metabroadcast.common.time.DateTimeZones;
import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.LookupRef;
import org.joda.time.DateTime;

import java.util.List;
import java.util.Set;

import static org.atlasapi.persistence.lookup.entry.EquivRefs.EquivDirection.BIDIRECTIONAL;

public class LookupEntry {

    public static LookupEntry lookupEntryFrom(Described c) {
        DateTime now = now();
        LookupRef lookupRef = LookupRef.from(c);
        EquivRefs reflexiveEquivRefs = EquivRefs.of(lookupRef, BIDIRECTIONAL);
        return new LookupEntry(
                c.getCanonicalUri(),
                c.getId(),
                lookupRef,
                c.getAllUris(),
                c.getAliases(),
                reflexiveEquivRefs,
                reflexiveEquivRefs,
                EquivRefs.of(),
                ImmutableSet.of(lookupRef),
                now,
                now,
                now,
                c.isActivelyPublished()
        );
    }

    private static DateTime now() {
        return new DateTime(DateTimeZones.UTC);
    }

    public static Function<LookupEntry,String> TO_ID = LookupEntry::uri;
    
    public static Function<LookupEntry, LookupRef> TO_SELF = LookupEntry::lookupRef;

    public static Function<LookupEntry,Set<LookupRef>> TO_EQUIVS = LookupEntry::equivalents;

    public static Function<LookupEntry,List<LookupRef>> TO_DIRECT_EQUIVS = input ->
            ImmutableList.copyOf(input.getDirectEquivalents().getLookupRefs());
    
    private final String uri;
    private final Long id;
    private final Set<String> aliasUris;
    private final Set<Alias> aliases;
    
    private final EquivRefs directEquivalents;
    private final EquivRefs explicitEquivalents;
    private final EquivRefs blacklistedEquivalents;
    private final Set<LookupRef> equivs;
    
    private final DateTime created;
    private final DateTime updated;
    private final DateTime equivUpdated;

    private final LookupRef self;
    private final boolean activelyPublished;

    public LookupEntry(
            String uri,
            Long id,
            LookupRef self,
            Set<String> aliasUris,
            Set<Alias> aliases,
            Set<LookupRef> directEquivs,
            Set<LookupRef> explicitEquivalents,
            Set<LookupRef> equivs,
            DateTime created,
            DateTime updated,
            boolean activelyPublished
    ) {
        this(
                uri,
                id,
                self,
                aliasUris,
                aliases,
                toEquivRefs(directEquivs, EquivRefs.EquivDirection.BIDIRECTIONAL),
                toEquivRefs(explicitEquivalents, EquivRefs.EquivDirection.BIDIRECTIONAL),
                EquivRefs.of(),
                ImmutableSet.copyOf(equivs),
                created,
                updated,
                null,
                activelyPublished
        );
        throw new UnsupportedOperationException("Tristan messed up, this should no longer be used");
    }

    private static EquivRefs toEquivRefs(Set<LookupRef> lookupRefs, EquivRefs.EquivDirection direction) {
        return EquivRefs.of(
                lookupRefs.stream()
                        .collect(MoreCollectors.toImmutableMap(lookupRef -> lookupRef, lookupRef -> direction))
        );
    }

    public LookupEntry(
            String uri,
            Long id,
            LookupRef self,
            Set<String> aliasUris,
            Set<Alias> aliases,
            EquivRefs directEquivs,
            EquivRefs explicitEquivalents,
            EquivRefs blacklistedEquivalents,
            Set<LookupRef> equivs,
            DateTime created,
            DateTime updated,
            DateTime equivUpdated,
            boolean activelyPublished
    ) {
        this.uri = uri;
        this.id = id;
        this.self = self;
        this.aliasUris = aliasUris;
        this.aliases = aliases;
        this.directEquivalents = directEquivs;
        this.explicitEquivalents = explicitEquivalents;
        this.blacklistedEquivalents = blacklistedEquivalents;
        this.equivs = equivs;
        this.created = created;
        this.updated = updated;
        this.equivUpdated = equivUpdated;
        this.activelyPublished = activelyPublished;
    }

    public String uri() {
        return uri;
    }
    
    public Long id() {
        return id;
    }

    public boolean activelyPublished() {
        return activelyPublished;
    }
    
    public Set<String> aliasUrls() {
        return aliasUris;
    }

    public Set<Alias> aliases() {
        return aliases;
    }
    
    public Set<String> identifiers() {
        return ImmutableSet.<String>builder().add(uri).addAll(aliasUris).build();
    }

    /**
     * @deprecated use {@link #getExplicitEquivalents()} instead
     */
    public Set<LookupRef> explicitEquivalents() {
        return explicitEquivalents.getLookupRefs();
    }

    public EquivRefs getExplicitEquivalents() {
        return explicitEquivalents;
    }

    public LookupEntry copyWithExplicitEquivalents(EquivRefs newExplicitEquivalents) {
        DateTime now = now();
        return new LookupEntry(
                uri,
                id,
                self,
                aliasUris,
                aliases,
                directEquivalents,
                newExplicitEquivalents.copyWithLink(self, BIDIRECTIONAL),
                blacklistedEquivalents,
                equivs,
                created,
                now,
                now,
                activelyPublished
        );
    }

    /**
     * @deprecated use {@link #getDirectEquivalents()} instead
     */
    public Set<LookupRef> directEquivalents() {
        return directEquivalents.getLookupRefs();
    }

    public EquivRefs getDirectEquivalents() {
        return directEquivalents;
    }

    public LookupEntry copyWithDirectEquivalents(EquivRefs newDirectEquivalents) {
        DateTime now = now();
        return new LookupEntry(
                uri,
                id,
                self,
                aliasUris,
                aliases,
                newDirectEquivalents.copyWithLink(self, BIDIRECTIONAL),
                explicitEquivalents,
                blacklistedEquivalents,
                equivs,
                created,
                now,
                now,
                activelyPublished
        );
    }

    public EquivRefs getBlacklistedEquivalents() {
        return blacklistedEquivalents;
    }

    public LookupEntry copyWithBlacklistedEquivalents(EquivRefs newBlacklistedEquivalents) {
        DateTime now = now();
        return new LookupEntry(
                uri,
                id,
                self,
                aliasUris,
                aliases,
                directEquivalents,
                explicitEquivalents,
                newBlacklistedEquivalents,
                equivs,
                created,
                now,
                now,
                activelyPublished
        );
    }

    public Set<LookupRef> equivalents() {
        return equivs;
    }

    public LookupEntry copyWithEquivalents(Set<LookupRef> newEquivalents) {
        Set<LookupRef> equivs = ImmutableSet.<LookupRef>builder()
                .addAll(newEquivalents)
                .add(self)
                .build();
        DateTime now = now();
        return new LookupEntry(
                uri,
                id,
                self,
                aliasUris,
                aliases,
                directEquivalents,
                explicitEquivalents,
                blacklistedEquivalents,
                equivs,
                created,
                now,
                now,
                activelyPublished
        );
    }

    public Set<LookupRef> getOutgoing() {
        return Sets.difference(
                Sets.union(explicitEquivalents.getOutgoing(), directEquivalents.getOutgoing()),
                blacklistedEquivalents.getOutgoing()
        );
    }

    public Set<LookupRef> getIncoming() {
        return Sets.difference(
                Sets.union(explicitEquivalents.getIncoming(), directEquivalents.getIncoming()),
                blacklistedEquivalents.getIncoming()
        );
    }

    public Set<LookupRef> getNeighbours() {
        return Sets.union(getOutgoing(), getIncoming());
    }

    public DateTime created() {
        return created;
    }

    public DateTime updated() {
        return updated;
    }

    public DateTime equivUpdated() {
        return equivUpdated;
    }

    public LookupRef lookupRef() {
        return self;
    }
    
    @Override
    public boolean equals(Object that) {
        if(this == that) {
            return true;
        }
        if(that instanceof LookupEntry) {
            LookupEntry other = (LookupEntry) that;
            return uri.equals(other.uri) && equivs.equals(other.equivs) && created.equals(other.created)
                    && updated.equals(other.updated) && equivUpdated.equals(other.equivUpdated)
                    && activelyPublished == other.activelyPublished;
        }
        return false;
    }
    
    @Override
    public int hashCode() {
        return Objects.hashCode(uri, equivs, created, updated, equivUpdated, activelyPublished);
    }
    
    @Override
    public String toString() {
        return "Lookup entry for " + uri;
    }

}
