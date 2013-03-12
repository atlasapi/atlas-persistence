package org.atlasapi.persistence.lookup.entry;

import org.atlasapi.media.common.Id;

@Deprecated
public interface LookupEntryStore {

    /**
     * Stores specified entry, and all related entries (like those for aliases).
     * 
     * @param entry
     */
    void store(LookupEntry entry);

    /**
     * Get entries for given URIs or Aliases (in case withAliases is set to true). There is a one-to-many mapping
     * from identifier to entry so more entries maybe returned than were
     * requested.
     * 
     * @param identifiers
     * @param withAliases
     * @return
     */
    Iterable<LookupEntry> entriesForIdentifiers(Iterable<String> identifiers, boolean withAliases);

    /**
     * Get entries for specified <b>canonical</b> URIs.
     * 
     * @param uris
     * @return
     */
    Iterable<LookupEntry> entriesForCanonicalUris(Iterable<String> uris);

    Iterable<LookupEntry> entriesForIds(Iterable<Id> ids);
}
