package org.atlasapi.persistence.lookup.entry;

import java.util.Map;

import org.atlasapi.media.entity.Publisher;

import com.google.common.base.Optional;
import com.metabroadcast.common.query.Selection;

public interface LookupEntryStore {

    /**
     * Stores specified entry, and all related entries (like those for aliases).
     * 
     * @param entry
     */
    void store(LookupEntry entry);

    /**
     * Get entries for given URIs or Aliases. There is a one-to-many mapping
     * from identifier to entry so more entries maybe returned than were
     * requested.
     * 
     * @param identifiers
     * @param useAliases TODO
     * @return
     */
    Iterable<LookupEntry> entriesForIdentifiers(Iterable<? extends String> identifiers, boolean useAliases);
    
    /**
     * Get entries for specified namespace and values.
     * 
     * @param namespace - not always present
     * @param values - one or more corresponding alias values
     * @return
     */
    Iterable<LookupEntry> entriesForAliases(Optional<String> namespace, Iterable<String> values);

    /**
     * Get entries for specified <b>canonical</b> URIs.
     * 
     * @param uris
     * @return
     */
    Iterable<LookupEntry> entriesForCanonicalUris(Iterable<? extends String> uris);

    Iterable<LookupEntry> entriesForIds(Iterable<Long> ids);

    Iterable<LookupEntry> entriesForPublishers(Iterable<Publisher> publishers, Selection selection);
    
    Map<String, Long> idsForCanonicalUris(Iterable<String> uris);
}
