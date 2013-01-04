package org.atlasapi.persistence.lookup.entry;

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
    Iterable<LookupEntry> entriesForIdentifiers(Iterable<String> identifiers, boolean useAliases);

    /**
     * Get entries for specified <b>canonical</b> URIs.
     * 
     * @param uris
     * @return
     */
    Iterable<LookupEntry> entriesForCanonicalUris(Iterable<String> uris);

    Iterable<LookupEntry> entriesForIds(Iterable<Long> ids);

}
