package org.atlasapi.persistence.content;

import static org.atlasapi.persistence.lookup.TransitiveLookupWriter.explicitTransitiveLookupWriter;

import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.lookup.TransitiveLookupWriter;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

public class EquivalenceWritingContentWriter implements ContentWriter {

    private final ContentWriter delegate;
    private final TransitiveLookupWriter equivalenceWriter;

    public EquivalenceWritingContentWriter(ContentWriter delegate, LookupEntryStore lookupEntryStore) {
        this.delegate = delegate;
        this.equivalenceWriter = explicitTransitiveLookupWriter(lookupEntryStore);
    }

    @Override
    public void createOrUpdate(Item item) {
        delegate.createOrUpdate(item);
        writeEquivalences(item);
    }

    private void writeEquivalences(Content content) {
        if (!content.getEquivalentTo().isEmpty()) {
            ImmutableSet<Publisher> publishers = publishers(content);
            Iterable<String> equivalentUris = Iterables.transform(content.getEquivalentTo(), LookupRef.TO_URI);
            equivalenceWriter.writeLookup(content.getCanonicalUri(), equivalentUris, publishers);
        }
    }

    private ImmutableSet<Publisher> publishers(Content content) {
        return ImmutableSet.<Publisher>builder().add(content.getPublisher()).addAll(Iterables.transform(content.getEquivalentTo(), LookupRef.TO_SOURCE)).build();
    }

    @Override
    public void createOrUpdate(Container container) {
        delegate.createOrUpdate(container);
        writeEquivalences(container);
    }

}
