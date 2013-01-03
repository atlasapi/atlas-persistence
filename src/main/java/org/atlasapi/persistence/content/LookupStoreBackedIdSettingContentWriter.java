package org.atlasapi.persistence.content;

import java.util.Iterator;

import org.atlasapi.media.entity.Content;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;

import com.google.common.collect.ImmutableList;
import com.metabroadcast.common.ids.IdGenerator;

public class LookupStoreBackedIdSettingContentWriter extends AbstractIdSettingContentWriter {

    protected final LookupEntryStore lookupStore;

    public LookupStoreBackedIdSettingContentWriter(LookupEntryStore lookupStore, IdGenerator generator, ContentWriter delegate) {
        super(generator, delegate);
        this.lookupStore = lookupStore;
    }

    @Override
    protected <T extends Content> Long getExistingId(T content) {
        Iterable<LookupEntry> entries = lookupStore.entriesForCanonicalUris(ImmutableList.of(content.getCanonicalUri()));
        Iterator<LookupEntry> entryIterator = entries.iterator();
        if (entryIterator.hasNext()) {
            return entryIterator.next().id();
        }
        return null;
    }

}
