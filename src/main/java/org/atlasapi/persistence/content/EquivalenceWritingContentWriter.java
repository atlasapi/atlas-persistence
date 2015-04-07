package org.atlasapi.persistence.content;

import org.atlasapi.equiv.ContentRef;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.lookup.LookupWriter;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

public class EquivalenceWritingContentWriter implements ContentWriter {

    private final ContentWriter delegate;
    private final LookupWriter equivalenceWriter;

    public EquivalenceWritingContentWriter(ContentWriter delegate, LookupWriter lookupWriter) {
        this.delegate = delegate;
        this.equivalenceWriter = lookupWriter;
    }

    @Override
    public Item createOrUpdate(Item item) {
        Item writtenItem = delegate.createOrUpdate(item);
        writeEquivalences(item);
        return writtenItem;
    }

    private void writeEquivalences(Content content) {
        if (!content.getEquivalentTo().isEmpty()) {
            ImmutableSet<Publisher> publishers = publishers(content);
            Iterable<ContentRef> equivalentUris = Iterables.transform(content.getEquivalentTo(),
                new Function<LookupRef, ContentRef>() {
                    @Override
                    public ContentRef apply(LookupRef input) {
                        return new ContentRef(input.uri(), input.publisher(), null);
                    }
                });
            equivalenceWriter.writeLookup(ContentRef.valueOf(content), equivalentUris, publishers);
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
