package org.atlasapi.persistence.lookup;

import java.util.Map;

import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.listing.ContentListingProgress;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.base.Predicates;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.metabroadcast.common.query.Selection;

public class InMemoryLookupEntryStore implements LookupEntryStore {

    private Map<String,LookupEntry> uriStore;
    private Map<Long,LookupEntry> idStore;
    private Multimap<String,LookupEntry> identifierStore;
    private Multimap<String, LookupEntry> aliasValueStore;
    private Multimap<Alias, LookupEntry> aliasStore;

    public InMemoryLookupEntryStore() {
        uriStore = Maps.newConcurrentMap();
        idStore = Maps.newConcurrentMap();
        identifierStore = ArrayListMultimap.create();
        aliasValueStore = ArrayListMultimap.create();
        aliasStore = ArrayListMultimap.create();
    }
    
    @Override
    public void store(LookupEntry entry) {
        uriStore.put(entry.uri(), entry);
        if (entry.id() != null) {
            idStore.put(entry.id(), entry);
        }
        for (String aliasUri : entry.aliasUrls()) {
            identifierStore.put(aliasUri, entry);
        }
        for (Alias alias : entry.aliases()) {
            aliasValueStore.put(alias.getValue(), entry);
            aliasStore.put(alias, entry);
        }
    }
    
    @Override
    public Iterable<LookupEntry> entriesForCanonicalUris(Iterable<String> uris) {
        return Iterables.filter(Iterables.transform(uris, Functions.forMap(uriStore, null)), Predicates.notNull());
    }

    @Override
    public Iterable<LookupEntry> entriesForIds(Iterable<Long> ids) {
        return Iterables.filter(Iterables.transform(ids, Functions.forMap(idStore, null)), Predicates.notNull());
    }

    @Override
    public Iterable<LookupEntry> entriesForIdentifiers(Iterable<String> identifiers, boolean useAliases) {
        return Iterables.concat(Iterables.filter(Iterables.transform(identifiers, Functions.forMap(identifierStore.asMap(),null)),Predicates.notNull()));
    }

    @Override
    public Iterable<LookupEntry> entriesForAliases(final Optional<String> namespace, Iterable<String> values) {
        if (namespace.isPresent()) {
            // create Aliases
            Iterable<Alias> aliases = Iterables.transform(values, new Function<String, Alias>() {
                @Override
                public Alias apply(String value) {
                    return new Alias(namespace.get(), value);
                }
            });
            return Iterables.concat(Iterables.filter(Iterables.transform(aliases, Functions.forMap(aliasStore.asMap(), null)), Predicates.notNull()));
        } else {
            return Iterables.concat(Iterables.filter(Iterables.transform(values, Functions.forMap(aliasValueStore.asMap(), null)), Predicates.notNull()));
        }
    }

    @Override
    public Map<String, Long> idsForCanonicalUris(Iterable<String> uris) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterable<LookupEntry> entriesForPublishers(Iterable<Publisher> publishers, Selection selection) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterable<LookupEntry> entriesForPublishers(Iterable<Publisher> publishers,
            ContentListingProgress progress, boolean onlyActivelyPublished) {
        throw new UnsupportedOperationException();
    }
}
