package org.atlasapi.persistence.lookup.entry;

import java.util.List;
import java.util.Set;

import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.LookupRef;
import org.joda.time.DateTime;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.metabroadcast.common.time.DateTimeZones;

public class LookupEntry {
    
    public static LookupEntry lookupEntryFrom(Described d) {
        DateTime now = new DateTime(DateTimeZones.UTC);
        LookupRef lookupRef = LookupRef.from(d);
        return new LookupEntry(d.getCanonicalUri(), lookupRef, d.getAliases(), ImmutableSet.of(lookupRef), ImmutableList.of(lookupRef), now, now);
    }
    
    public static Function<LookupEntry,String> TO_ID = new Function<LookupEntry, String>() {
        @Override
        public String apply(LookupEntry input) {
            return input.id();
        }
    };
    
    public static Function<LookupEntry,List<LookupRef>> TO_EQUIVS = new Function<LookupEntry, List<LookupRef>>() {
        @Override
        public List<LookupRef> apply(LookupEntry input) {
            return input.equivalents();
        }
    };
    
    public static Function<LookupEntry,List<LookupRef>> TO_DIRECT_EQUIVS = new Function<LookupEntry, List<LookupRef>>() {
        @Override
        public List<LookupRef> apply(LookupEntry input) {
            return ImmutableList.copyOf(input.directEquivalents());
        }
    };
    
    private final String id;
    private final Set<String> aliases;
    
    private final Set<LookupRef> directEquivalents;
    private final List<LookupRef> equivs;
    
    private final DateTime created;
    private final DateTime updated;

    private final LookupRef self;

    public LookupEntry(String id, LookupRef self, Set<String> aliases, Set<LookupRef> directEquivs, List<LookupRef> equivs, DateTime created, DateTime updated) {
        this.id = id;
        this.self = self;
        this.aliases = aliases;
        this.directEquivalents = ImmutableSet.copyOf(directEquivs);
        this.equivs = ImmutableList.copyOf(equivs);
        this.created = created;
        this.updated = updated;
    }

    public String id() {
        return id;
    }

    public Set<String> aliases() {
        return aliases;
    }
    
    public Set<String> identifiers() {
        return ImmutableSet.<String>builder().add(id).addAll(aliases).build();
    }
    
    public List<LookupRef> equivalents() {
        return equivs;
    }

    public LookupEntry copyWithEquivalents(Iterable<LookupRef> newEquivs) {
        return new LookupEntry(id, self, aliases, directEquivalents, ImmutableList.copyOf(newEquivs), created, new DateTime(DateTimeZones.UTC));
    }
    
    public Set<LookupRef> directEquivalents() {
        return directEquivalents;
    }
    
    public LookupEntry copyWithDirectEquivalents(Iterable<LookupRef> directEquivalents) {
        return new LookupEntry(id, self, aliases, ImmutableSet.copyOf(directEquivalents), equivs, created, new DateTime(DateTimeZones.UTC));
    }

    public DateTime created() {
        return created;
    }

    public DateTime updated() {
        return updated;
    }
    
    public List<LookupEntry> entriesForIdentifiers() {
        List<LookupEntry> entries = Lists.newArrayList(this);
        for (String alias : aliases) {
            entries.add(new LookupEntry(alias, self, ImmutableSet.<String>of(), ImmutableSet.<LookupRef>of(), this.equivs, created, updated));
        }
        return ImmutableList.copyOf(entries);
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
            return id.equals(other.id) && equivs.equals(other.equivs) && created.equals(other.created) && updated.equals(other.updated);
        }
        return false;
    }
    
    @Override
    public int hashCode() {
        return Objects.hashCode(id, equivs, created, updated);
    }
    
    @Override
    public String toString() {
        return "Lookup entry for " + id;
    }

}
