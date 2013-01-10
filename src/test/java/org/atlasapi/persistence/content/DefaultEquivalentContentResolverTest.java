package org.atlasapi.persistence.content;

import static org.hamcrest.Matchers.hasItems;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.Annotation;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

public class DefaultEquivalentContentResolverTest {

    private final ContentResolver contentResolver = mock(ContentResolver.class);
    private final LookupEntryStore lookupResolver = mock(LookupEntryStore.class);
    private final EquivalentContentResolver equivResolver = new DefaultEquivalentContentResolver(contentResolver, lookupResolver);
    
    @Test
    public void testResolvesSimpleContent() {
        
        Episode subject = new Episode("testUri", "testCurie", Publisher.BBC);
        Episode equiv = new Episode("equiv", "equivCurie", Publisher.PA);
        
        Iterable<String> uris = ImmutableSet.of(subject.getCanonicalUri());
        Set<Publisher> selectedSources = ImmutableSet.of(Publisher.BBC, Publisher.PA);
        Set<Annotation> annotations = Annotation.defaultAnnotations();
        
        when(lookupResolver.entriesForIdentifiers(uris, false)).thenReturn(ImmutableSet.of(
            LookupEntry.lookupEntryFrom(subject).copyWithEquivalents(ImmutableSet.of(
                LookupRef.from(equiv)
            ))
        ));
        when(contentResolver.findByCanonicalUris(argThat(hasItems("testUri","equiv"))))
            .thenReturn(ResolvedContent.builder()
                .put(subject.getCanonicalUri(), subject)
                .put(equiv.getCanonicalUri(), equiv)
                .build());
        
        EquivalentContent content = equivResolver.resolveUris(uris, selectedSources, annotations, false);
        
        assertEquals(1, content.asMap().size());
        assertNull(content.asMap().get("equiv"));
        
        Collection<Content> equivContent = content.asMap().get("testUri");
        assertNotNull(equivContent);
        Map<String,Content> equivMap = Maps.uniqueIndex(equivContent, Identified.TO_URI);
        assertEquals(2, equivMap.size());
        assertEquals(subject, equivMap.get(subject.getCanonicalUri()));
        assertEquals(equiv, equivMap.get(equiv.getCanonicalUri()));
    }

    @Test
    public void testResolvesContentWithTwoKeys() {
        
        Episode subject1 = new Episode("testUri1", "testCurie", Publisher.BBC);
        Episode subject2 = new Episode("testUri2", "testCurie", Publisher.BBC);
        Episode equiv = new Episode("equiv", "equivCurie", Publisher.PA);
        
        Iterable<String> uris = ImmutableSet.of(subject1.getCanonicalUri(), subject2.getCanonicalUri());
        Set<Publisher> selectedSources = ImmutableSet.of(Publisher.BBC, Publisher.PA);
        Set<Annotation> annotations = Annotation.defaultAnnotations();
        
        when(lookupResolver.entriesForIdentifiers(uris, false)).thenReturn(ImmutableSet.of(
            LookupEntry.lookupEntryFrom(subject1).copyWithEquivalents(ImmutableSet.of(
                LookupRef.from(equiv)
            )),
            LookupEntry.lookupEntryFrom(subject2)
        ));
        when(contentResolver.findByCanonicalUris(argThat(hasItems("testUri1","equiv","testUri2"))))
            .thenReturn(ResolvedContent.builder()
                .put(subject1.getCanonicalUri(), subject1)
                .put(subject2.getCanonicalUri(), subject2)
                .put(equiv.getCanonicalUri(), equiv)
                .build());
        
        EquivalentContent content = equivResolver.resolveUris(uris, selectedSources, annotations, false);
        
        assertEquals(2, content.asMap().size());
        assertNull(content.asMap().get("equiv"));
        
        Collection<Content> equivContent = content.asMap().get("testUri1");
        assertNotNull(equivContent);
        Map<String,Content> equivMap = Maps.uniqueIndex(equivContent, Identified.TO_URI);
        assertEquals(2, equivMap.size());
        assertEquals(subject1, equivMap.get(subject1.getCanonicalUri()));
        assertEquals(equiv, equivMap.get(equiv.getCanonicalUri()));
        
        equivContent = content.asMap().get("testUri2");
        assertNotNull(equivContent);
        equivMap = Maps.uniqueIndex(equivContent, Identified.TO_URI);
        assertEquals(1, equivMap.size());
        assertEquals(subject2, equivMap.get(subject2.getCanonicalUri()));
    }
}
