package org.atlasapi.persistence.content;

import org.atlasapi.media.entity.Content;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;

import com.metabroadcast.common.ids.IdGenerator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import static com.google.common.base.Preconditions.checkNotNull;

public class LookupBackedContentIdGenerator {

    private final LookupEntryStore lookupStore;
    private final IdGenerator idGenerator;

    public LookupBackedContentIdGenerator(LookupEntryStore lookupStore, IdGenerator idGenerator) {
        this.lookupStore = checkNotNull(lookupStore);
        this.idGenerator = checkNotNull(idGenerator);
    }

    public <T extends Content> Long getId(T content) {
        Iterable<LookupEntry> entries = lookupStore
                .entriesForCanonicalUris(ImmutableList.of(content.getCanonicalUri()));

        if (Iterables.isEmpty(entries)) {
            return idGenerator.generateRaw();
        } else {
            //ensures an adapter can't override and assign a new id for the content.
            return Iterables.getOnlyElement(entries).id();
        }
    }
}
