package org.atlasapi.persistence.content;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.atlasapi.application.ApplicationConfiguration;
import org.atlasapi.application.SourceStatus;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.Annotation;
import org.atlasapi.persistence.lookup.InMemoryLookupEntryStore;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

public class DefaultEquivalentContentResolverTest {

    private DummyKnownTypeContentResolver contentResolver;
    private LookupEntryStore lookupResolver;
    private EquivalentContentResolver equivResolver;
    
    @Before
    public void setUp() {
        contentResolver = new DummyKnownTypeContentResolver();
        lookupResolver = new InMemoryLookupEntryStore();
        equivResolver = new DefaultEquivalentContentResolver(contentResolver, lookupResolver);
    }
    
    @Test
    public void testResolvesSimpleContent() {
        
        Episode subject = episode("testUri", 1, Publisher.BBC);
        Episode equiv = episode("equiv", 2, Publisher.PA);
        
        Iterable<String> uris = ImmutableSet.of(subject.getCanonicalUri());
        ApplicationConfiguration appConfig = configWithSources(Publisher.PA, Publisher.BBC);
        Set<Annotation> annotations = Annotation.defaultAnnotations();
        
        lookupResolver.store(entry(subject, ImmutableSet.of(equiv), equiv));
        contentResolver.respondTo(ImmutableSet.of(subject, equiv));
        
        EquivalentContent content = equivResolver.resolveUris(uris, appConfig, annotations, false);
        
        assertEquals(1, content.asMap().size());
        assertNull(content.asMap().get(equiv.getCanonicalUri()));
        
        checkEquivContent(content, subject, equiv);
    }

    @Test
    public void testResolvesContentWithTwoKeys() {
        
        Episode subject1 = episode("testUri1", 1, Publisher.BBC);
        Episode subject2 = episode("testUri2", 2, Publisher.BBC);
        Episode equiv = episode("equiv", 3, Publisher.PA);
        
        Iterable<String> uris = ImmutableSet.of(subject1.getCanonicalUri(), subject2.getCanonicalUri());
        ApplicationConfiguration appConfig = configWithSources(Publisher.PA, Publisher.BBC);
        Set<Annotation> annotations = Annotation.defaultAnnotations();
        
        lookupResolver.store(entry(subject1, ImmutableSet.of(equiv), equiv));
        lookupResolver.store(entry(subject2, ImmutableSet.<Content>of()));
        contentResolver.respondTo(ImmutableSet.of(subject1, subject2, equiv));
        
        EquivalentContent content = equivResolver.resolveUris(uris, appConfig, annotations, false);
        
        assertEquals(2, content.asMap().size());
        assertNull(content.asMap().get(equiv.getCanonicalUri()));
        
        checkEquivContent(content, subject1, equiv);
        checkEquivContent(content, subject2);
    }
    
    @Test
    public void testReturnsOnlyRequestedContentFromPrecedentSourceAndRemovesAdjacentsOfThoseRemoved() {
        
        Episode subject = episode("subject", 4, Publisher.PA);
        Episode equiv = episode("equiv", 2, Publisher.BBC);
        Episode filtered = episode("filtered", 1, Publisher.PA);
        Episode filteredEquiv = episode("filteredEquiv", 3, Publisher.C4);
        
        Iterable<String> uris = ImmutableSet.of(subject.getCanonicalUri());
        ApplicationConfiguration appConfig = configWithSources(Publisher.PA, Publisher.BBC, Publisher.C4)
                .copyWithPrecedence(ImmutableList.of(Publisher.PA, Publisher.BBC, Publisher.C4));
        Set<Annotation> annotations = Annotation.defaultAnnotations();
        
        lookupResolver.store(entry(subject, ImmutableSet.of(equiv), 
                equiv, filtered, filteredEquiv));
        lookupResolver.store(entry(filtered, ImmutableSet.of(filteredEquiv),
                subject, equiv, filteredEquiv));
        contentResolver.respondTo(ImmutableSet.of(subject, equiv, filtered, filteredEquiv));
        
        EquivalentContent content = equivResolver.resolveUris(uris, appConfig, annotations, false);
        
        assertEquals(1, content.asMap().size());
        assertNull(content.asMap().get(equiv.getCanonicalUri()));
        
        checkEquivContent(content, subject, equiv);
    }

    @Test
    public void testCanLookupTwoThingsWhereEachIsRemovedFromTheOther() {
        
        Episode subject = episode("subject", 4, Publisher.PA);
        Episode equiv = episode("equiv", 2, Publisher.BBC);
        Episode filtered = episode("filtered", 1, Publisher.PA);
        Episode filteredEquiv = episode("filteredEquiv", 3, Publisher.C4);
        
        Iterable<String> uris = ImmutableSet.of(subject.getCanonicalUri(),
                filtered.getCanonicalUri());
        ApplicationConfiguration appConfig = configWithSources(Publisher.PA, Publisher.BBC, Publisher.C4)
                .copyWithPrecedence(ImmutableList.of(Publisher.PA, Publisher.BBC, Publisher.C4));
        Set<Annotation> annotations = Annotation.defaultAnnotations();
        
        lookupResolver.store(entry(subject, ImmutableSet.of(equiv), 
            equiv, filtered, filteredEquiv));
        lookupResolver.store(entry(filtered, ImmutableSet.of(filteredEquiv),
            subject, equiv, filteredEquiv));
        contentResolver.respondTo(ImmutableSet.of(subject, equiv, filtered, filteredEquiv));
        
        EquivalentContent content = equivResolver.resolveUris(uris, appConfig, annotations, false);
        
        assertEquals(2, content.asMap().size());
        assertNull(content.asMap().get(equiv.getCanonicalUri()));
        assertNull(content.asMap().get(filteredEquiv.getCanonicalUri()));
        
        checkEquivContent(content, subject, equiv);
        checkEquivContent(content, filtered, filteredEquiv);
    }

    @Test
    public void testOnlyResolvesLowestItemFromPrecedentSourceWhenNonPrecedentSourceItemRequested() {
        
        Episode subject = episode("subject", 4, Publisher.PA);
        Episode equiv = episode("equiv", 2, Publisher.BBC);
        Episode filtered = episode("filtered", 1, Publisher.PA);
        Episode filteredEquiv = episode("filteredEquiv", 3, Publisher.C4);
        
        Iterable<String> uris = ImmutableSet.of(equiv.getCanonicalUri(),
                filteredEquiv.getCanonicalUri());
        ApplicationConfiguration appConfig = configWithSources(Publisher.PA, Publisher.BBC, Publisher.C4)
                .copyWithPrecedence(ImmutableList.of(Publisher.PA, Publisher.BBC, Publisher.C4));
        Set<Annotation> annotations = Annotation.defaultAnnotations();
        
        lookupResolver.store(entry(subject, ImmutableSet.of(equiv), 
                equiv, filtered, filteredEquiv));
        lookupResolver.store(entry(equiv, ImmutableSet.of(subject), 
                subject, filtered, filteredEquiv));
        lookupResolver.store(entry(filtered, ImmutableSet.of(filteredEquiv),
                subject, equiv, filteredEquiv));
        lookupResolver.store(entry(filteredEquiv, ImmutableSet.of(filtered),
                subject, equiv, filtered));
        contentResolver.respondTo(ImmutableSet.of(subject, equiv, filtered, filteredEquiv));
        
        EquivalentContent content = equivResolver.resolveUris(uris, appConfig, annotations, false);
        
        assertEquals(2, content.asMap().size());
        assertNull(content.asMap().get(subject.getCanonicalUri()));
        assertNull(content.asMap().get(filtered.getCanonicalUri()));
        
        checkEquivContent(content, equiv, filtered, filteredEquiv);
        checkEquivContent(content, filteredEquiv, filtered);
    }

    private ApplicationConfiguration configWithSources(Publisher... srcs) {
        ApplicationConfiguration conf = ApplicationConfiguration.defaultConfiguration();
        for (Publisher src : srcs) {
            conf = conf.withSource(src, SourceStatus.AVAILABLE_ENABLED);
        }
        return conf;
    }

    private <C extends Content> void checkEquivContent(EquivalentContent content, C subj, C...equivs) {
        Collection<Content> equivContent = content.asMap().get(subj.getCanonicalUri());
        assertNotNull(equivContent);
        Map<String,Content> equivMap = Maps.uniqueIndex(equivContent, Identified.TO_URI);
        assertEquals(equivs.length+1, equivMap.size());
        assertEquals(subj, equivMap.get(subj.getCanonicalUri()));
        for (C eq : equivs) {
            assertEquals(eq, equivMap.get(eq.getCanonicalUri()));
        }
    }
    
    private LookupEntry entry(Content c, Iterable<? extends Content> direct, Content... equivs) {
        return LookupEntry.lookupEntryFrom(c)
            .copyWithDirectEquivalents(Iterables.transform(direct, LookupRef.FROM_DESCRIBED))
            .copyWithEquivalents(Collections2.transform(ImmutableSet.copyOf(equivs), LookupRef.FROM_DESCRIBED));
    }
    
    private Episode episode(String uri, long id, Publisher src) {
        Episode ep = new Episode(uri, uri, src);
        ep.setId(id);
        return ep;
    }
    
}
