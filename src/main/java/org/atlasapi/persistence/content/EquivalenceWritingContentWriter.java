package org.atlasapi.persistence.content;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.atlasapi.equiv.ContentRef;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.lookup.LookupWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EquivalenceWritingContentWriter implements EquivalentContentWriter {

    private static final Logger log = LoggerFactory.getLogger(EquivalenceWritingContentWriter.class);
    private static final Logger timerLog = LoggerFactory.getLogger("TIMER");

    private final ContentWriter delegate;
    private final LookupWriter equivalenceWriter;

    public EquivalenceWritingContentWriter(ContentWriter delegate, LookupWriter lookupWriter) {
        this.delegate = delegate;
        this.equivalenceWriter = lookupWriter;
    }

    @Override
    public Item createOrUpdate(Item item) {
        return createOrUpdate(item, false);
    }

    @Override
    public Item createOrUpdate(Item item, boolean writeEquivalentsIfEmpty) {
        Long lastTime = System.nanoTime();
        timerLog.debug("TIMER EQ entered. {} {}",item.getId(), Thread.currentThread().getName());
        Item writtenItem = delegate.createOrUpdate(item);
        timerLog.debug("TIMER EQ Delegate finished "+Long.toString((System.nanoTime() - lastTime)/1000000)+"ms. {} {}",item.getId(), Thread.currentThread().getName());
        lastTime = System.nanoTime();
        writeEquivalences(item, writeEquivalentsIfEmpty);
        timerLog.debug("TIMER EQ Local work finished "+Long.toString((System.nanoTime() - lastTime)/1000000)+"ms. {} {}",item.getId(), Thread.currentThread().getName());
        return writtenItem;
    }

    private void writeEquivalences(Content content, boolean writeEmptyEquivalents) {
        if (writeEmptyEquivalents || !content.getEquivalentTo().isEmpty()) {
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
        createOrUpdate(container, false);
    }

    @Override
    public void createOrUpdate(Container container, boolean writeEquivalentsIfEmpty) {
        Long lastTime = System.nanoTime();
        timerLog.debug("TIMER EQ entered. {} {}",container.getId(), Thread.currentThread().getName());
        delegate.createOrUpdate(container);
        timerLog.debug("TIMER EQ Delegate finished "+Long.toString((System.nanoTime() - lastTime)/1000000)+"ms. {} {}",container.getCanonicalUri(), Thread.currentThread().getName());
        lastTime = System.nanoTime();
        writeEquivalences(container, writeEquivalentsIfEmpty);
        timerLog.debug("TIMER EQ Local work finished "+Long.toString((System.nanoTime() - lastTime)/1000000)+"ms. {} {}",container.getCanonicalUri(), Thread.currentThread().getName());
    }

}
