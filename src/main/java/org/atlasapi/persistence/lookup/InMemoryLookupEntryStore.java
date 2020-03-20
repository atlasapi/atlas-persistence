package org.atlasapi.persistence.lookup;

import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.base.Predicates;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.metabroadcast.common.query.Selection;
import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.Transaction;
import org.atlasapi.persistence.content.listing.ContentListingProgress;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

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
        return entriesForAliases(namespace, values, true);
    }

    @Override
    public Iterable<LookupEntry> entriesForAliases(
            final Optional<String> namespace,
            Iterable<String> values,
            boolean includeUnpublishedEntries
    ) {
        return entriesForAliases(namespace, values, null, includeUnpublishedEntries);
    }

    @Override
    public Iterable<LookupEntry> entriesForAliases(
            final Optional<String> namespace,
            Iterable<String> values,
            @Nullable Iterable<Publisher> publishers,
            boolean includeUnpublishedEntries
    ) {
        Iterable<LookupEntry> lookupEntries;
        if (namespace.isPresent()) {
            // create Aliases
            Iterable<Alias> aliases = StreamSupport.stream(values.spliterator(), false)
                    .map(value -> new Alias(namespace.get(), value))
                    .collect(Collectors.toList());
            lookupEntries = Iterables.concat(
                    Iterables.filter(
                            Iterables.transform(aliases, Functions.forMap(aliasStore.asMap(), null)
                            ),
                            Predicates.notNull()
                    )
            );
        } else {
            lookupEntries = Iterables.concat(
                    Iterables.filter(
                            Iterables.transform(values, Functions.forMap(aliasValueStore.asMap(), null)
                            ),
                            Predicates.notNull()
                    )
            );
        }

        lookupEntries = Iterables.filter(
                lookupEntries,
                lookupEntry -> includeUnpublishedEntries || (lookupEntry != null && lookupEntry.activelyPublished())
        );

        lookupEntries = Iterables.filter(
                lookupEntries,
                lookupEntry -> lookupEntry.lookupRef() != null
                        && (publishers == null || Iterables.contains(publishers, lookupEntry.lookupRef().publisher()))
        );

        return lookupEntries;
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
    public Iterable<LookupEntry> allEntriesForPublishers(Iterable<Publisher> publishers,
            ContentListingProgress progress) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterable<LookupEntry> updatedSince(Publisher publisher, DateTime dateTime) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterable<LookupEntry> equivUpdatedSince(Publisher publisher, DateTime dateTime) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Transaction startTransaction() {
        return null;
    }

    @Override
    public void store(Transaction transaction, LookupEntry entry) {
        store(entry);
    }

    @Override
    public Iterable<LookupEntry> entriesForCanonicalUris(Transaction transaction, Iterable<String> uris) {
        return entriesForCanonicalUris(uris);
    }

    @Override
    public Iterable<LookupEntry> entriesForIds(Transaction transaction, Iterable<Long> ids) {
        return entriesForIds(ids);
    }
}
