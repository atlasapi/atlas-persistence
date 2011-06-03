package org.atlasapi.persistence.lookup.entry;

import java.util.List;
import java.util.Set;

import org.atlasapi.media.entity.Described;
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
        return new LookupEntry(d.getCanonicalUri(), d.getAliases(), ImmutableSet.of(Equivalent.from(d)), ImmutableList.of(Equivalent.from(d)), now, now);
    }
    
    public static Function<LookupEntry,String> TO_ID = new Function<LookupEntry, String>() {
        @Override
        public String apply(LookupEntry input) {
            return input.id();
        }
    };
    
    public static Function<LookupEntry,List<Equivalent>> TO_EQUIVS = new Function<LookupEntry, List<Equivalent>>() {
        @Override
        public List<Equivalent> apply(LookupEntry input) {
            return input.equivalents();
        }
    };
    
    private final String id;
    private final Set<String> aliases;
    
    private final Set<Equivalent> directEquivalents;
    private final List<Equivalent> equivs;
    
    private final DateTime created;
    private final DateTime updated;

    public LookupEntry(String id, Set<String> aliases, Set<Equivalent> directEquivs, List<Equivalent> equivs, DateTime created, DateTime updated) {
        this.id = id;
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
    
    public List<Equivalent> equivalents() {
        return equivs;
    }

    public LookupEntry copyWithEquivalents(Iterable<Equivalent> newEquivs) {
        return new LookupEntry(id, aliases, directEquivalents, ImmutableList.copyOf(newEquivs), created, new DateTime(DateTimeZones.UTC));
    }
    
    public Set<Equivalent> directEquivalents() {
        return directEquivalents;
    }
    
    public LookupEntry copyWithDirectEquivalents(Iterable<Equivalent> directEquivalents) {
        return new LookupEntry(id, aliases, ImmutableSet.copyOf(directEquivalents), equivs, created, new DateTime(DateTimeZones.UTC));
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
            entries.add(new LookupEntry(alias, ImmutableSet.<String>of(), ImmutableSet.<Equivalent>of(), this.equivs, created, updated));
        }
        return ImmutableList.copyOf(entries);
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
