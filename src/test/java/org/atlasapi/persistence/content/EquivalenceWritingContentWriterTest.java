package org.atlasapi.persistence.content;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.lookup.InMemoryLookupEntryStore;
import org.atlasapi.persistence.lookup.LookupWriter;
import org.atlasapi.persistence.lookup.TransitiveLookupWriter;
import org.atlasapi.persistence.lookup.entry.EquivRefs;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.junit.Test;

import static org.atlasapi.persistence.lookup.entry.EquivRefs.EquivDirection.OUTGOING;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class EquivalenceWritingContentWriterTest {

    private final LookupEntryStore lookupEntryStore = new InMemoryLookupEntryStore();
    private final ContentWriter delegate = mock(ContentWriter.class);
    private final LookupWriter lookupWriter = TransitiveLookupWriter.explicitTransitiveLookupWriter(lookupEntryStore);
    
    private final EquivalenceWritingContentWriter contentWriter = new EquivalenceWritingContentWriter(delegate, lookupWriter);

    @Test
    public void testWritingContentWithEquivalencesWritesEquivalences() {

        Item firstSubjectItem = new Item("subjectUri", "curie", Publisher.METABROADCAST);
        LookupEntry subject = LookupEntry.lookupEntryFrom(firstSubjectItem);
        lookupEntryStore.store(subject);

        Item generatedEquivalent = new Item("generatedUri", "curie", Publisher.FIVE);
        LookupEntry generatedEquiv = LookupEntry.lookupEntryFrom(generatedEquivalent);
        
        Item explicitEquivalent = new Item("equivUri", "durie", Publisher.BBC);
        LookupEntry explicitEquiv = LookupEntry.lookupEntryFrom(explicitEquivalent)
                .copyWithDirectEquivalents(EquivRefs.of(generatedEquiv.lookupRef(), OUTGOING))
                .copyWithEquivalents(ImmutableSet.of(generatedEquiv.lookupRef()));
        lookupEntryStore.store(explicitEquiv);

        generatedEquiv = generatedEquiv
                .copyWithDirectEquivalents(EquivRefs.of(explicitEquiv.lookupRef(), OUTGOING))
                .copyWithExplicitEquivalents(EquivRefs.of(explicitEquiv.lookupRef(), OUTGOING));
        lookupEntryStore.store(generatedEquiv);
        
        firstSubjectItem.setEquivalentTo(ImmutableSet.of(explicitEquiv.lookupRef()));
        
        contentWriter.createOrUpdate(firstSubjectItem);

        verify(delegate).createOrUpdate(firstSubjectItem);
        
        LookupEntry first = Iterables.getOnlyElement(lookupEntryStore.entriesForCanonicalUris(ImmutableSet.of(firstSubjectItem.getCanonicalUri())));
        assertTrue(first.explicitEquivalents().getLookupRefs().contains(explicitEquiv.lookupRef()));
        assertTrue(first.equivalents().contains(explicitEquiv.lookupRef()));

        LookupEntry explicit = Iterables.getOnlyElement(lookupEntryStore.entriesForCanonicalUris(ImmutableSet.of(explicitEquivalent.getCanonicalUri())));
        assertTrue(explicit.explicitEquivalents().getLookupRefs().contains(subject.lookupRef()));
        assertTrue(explicit.directEquivalents().getLookupRefs().contains(generatedEquiv.lookupRef()));
        assertTrue(explicit.equivalents().contains(subject.lookupRef()));
        assertTrue(explicit.equivalents().contains(generatedEquiv.lookupRef()));

        LookupEntry generated = Iterables.getOnlyElement(lookupEntryStore.entriesForCanonicalUris(ImmutableSet.of(generatedEquivalent.getCanonicalUri())));
        assertTrue(generated.directEquivalents().getLookupRefs().contains(explicitEquiv.lookupRef()));
        assertTrue(generated.equivalents().contains(subject.lookupRef()));
        assertTrue(generated.equivalents().contains(explicitEquiv.lookupRef()));
        
        
        Item secondSubjectItem = new Item("secondUri", "eurie", Publisher.C4);
        LookupEntry secondSubject = LookupEntry.lookupEntryFrom(secondSubjectItem);
        lookupEntryStore.store(secondSubject);
        
        secondSubjectItem.setEquivalentTo(ImmutableSet.of(explicitEquiv.lookupRef()));
        
        contentWriter.createOrUpdate(secondSubjectItem);

        verify(delegate).createOrUpdate(secondSubjectItem);
        
        LookupEntry second = Iterables.getOnlyElement(lookupEntryStore.entriesForCanonicalUris(ImmutableSet.of(secondSubjectItem.getCanonicalUri())));
        assertTrue(second.explicitEquivalents().getLookupRefs().contains(explicitEquiv.lookupRef()));
        assertTrue(second.equivalents().contains(explicitEquiv.lookupRef()));
        assertTrue(second.equivalents().contains(subject.lookupRef()));
        assertTrue(second.equivalents().contains(generatedEquiv.lookupRef()));
        
        first = Iterables.getOnlyElement(lookupEntryStore.entriesForCanonicalUris(ImmutableSet.of(firstSubjectItem.getCanonicalUri())));
        assertTrue(first.explicitEquivalents().getLookupRefs().contains(explicitEquiv.lookupRef()));
        assertTrue(first.equivalents().contains(explicitEquiv.lookupRef()));
        assertTrue(first.equivalents().contains(secondSubject.lookupRef()));
        assertTrue(first.equivalents().contains(generatedEquiv.lookupRef()));
        
        explicit = Iterables.getOnlyElement(lookupEntryStore.entriesForCanonicalUris(ImmutableSet.of(explicitEquivalent.getCanonicalUri())));
        assertTrue(explicit.explicitEquivalents().getLookupRefs().contains(subject.lookupRef()));
        assertTrue(explicit.explicitEquivalents().getLookupRefs().contains(secondSubject.lookupRef()));
        assertTrue(explicit.equivalents().contains(subject.lookupRef()));
        assertTrue(explicit.equivalents().contains(secondSubject.lookupRef()));
        assertTrue(explicit.equivalents().contains(generatedEquiv.lookupRef()));

        generated = Iterables.getOnlyElement(lookupEntryStore.entriesForCanonicalUris(ImmutableSet.of(generatedEquivalent.getCanonicalUri())));
        assertTrue(explicit.equivalents().contains(subject.lookupRef()));
        assertTrue(explicit.equivalents().contains(secondSubject.lookupRef()));
        assertTrue(explicit.equivalents().contains(explicitEquiv.lookupRef()));
    }

}
