package org.atlasapi.persistence.content;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.metabroadcast.applications.client.model.internal.Application;
import com.metabroadcast.applications.client.model.internal.ApplicationConfiguration;
import com.metabroadcast.common.base.MorePredicates;
import com.metabroadcast.common.stream.MoreCollectors;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.Annotation;
import org.atlasapi.persistence.lookup.InMemoryLookupEntryStore;
import org.atlasapi.persistence.lookup.entry.EquivRefs;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import static org.atlasapi.persistence.lookup.entry.EquivRefs.Direction.OUTGOING;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DefaultEquivalentContentResolverTest {

    private DummyKnownTypeContentResolver contentResolver;
    private LookupEntryStore lookupResolver;
    private EquivalentContentResolver equivResolver;
    private Application application = mock(Application.class);
    
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
        Set<Annotation> annotations = Annotation.defaultAnnotations();

        when(application.getConfiguration()).thenReturn(configWithSources(Publisher.PA, Publisher.BBC));

        lookupResolver.store(entry(subject, ImmutableSet.of(equiv), equiv));
        contentResolver.respondTo(ImmutableSet.of(subject, equiv));
        
        EquivalentContent content = equivResolver.resolveUris(uris, application, annotations, false);
        
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
        Set<Annotation> annotations = Annotation.defaultAnnotations();

        when(application.getConfiguration()).thenReturn(configWithSources(Publisher.PA, Publisher.BBC));

        lookupResolver.store(entry(subject1, ImmutableSet.of(equiv), equiv));
        lookupResolver.store(entry(subject2, ImmutableSet.<Content>of()));
        contentResolver.respondTo(ImmutableSet.of(subject1, subject2, equiv));
        
        EquivalentContent content = equivResolver.resolveUris(uris, application, annotations, false);
        
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
        Episode invisible = episode("invisible", 5, Publisher.FIVE);
        
        Iterable<String> uris = ImmutableSet.of(subject.getCanonicalUri());
        Set<Annotation> annotations = Annotation.defaultAnnotations();

        when(application.getConfiguration()).thenReturn(configWithSources(Publisher.PA, Publisher.BBC, Publisher.C4));

        lookupResolver.store(entry(subject, ImmutableSet.of(equiv, invisible),
                equiv, filtered, filteredEquiv, invisible));
        lookupResolver.store(entry(filtered, ImmutableSet.of(filteredEquiv),
                subject, equiv, filteredEquiv));
        contentResolver.respondTo(ImmutableSet.of(subject, equiv, filtered, filteredEquiv));
        
        EquivalentContent content = equivResolver.resolveUris(uris, application, annotations, false);
        
        assertEquals(1, content.asMap().size());
        assertNull(content.asMap().get(equiv.getCanonicalUri()));
        
        checkEquivContent(content, subject, equiv);
        assertEquals(ImmutableSet.copyOf(subject.getEquivalentTo()),
            ImmutableSet.copyOf(Iterables.transform(ImmutableList.of(subject, equiv, invisible), LookupRef.FROM_DESCRIBED))
        );
    }

    @Test
    public void testCanLookupTwoThingsWhereEachIsRemovedFromTheOther() {
        
        Episode subject = episode("subject", 4, Publisher.PA);
        Episode equiv = episode("equiv", 2, Publisher.BBC);
        Episode filtered = episode("filtered", 1, Publisher.PA);
        Episode filteredEquiv = episode("filteredEquiv", 3, Publisher.C4);
        
        Iterable<String> uris = ImmutableSet.of(subject.getCanonicalUri(),
                filtered.getCanonicalUri());
        Set<Annotation> annotations = Annotation.defaultAnnotations();

        when(application.getConfiguration()).thenReturn(configWithSources(Publisher.PA, Publisher.BBC, Publisher.C4));

        lookupResolver.store(entry(subject, ImmutableSet.of(equiv), 
            equiv, filtered, filteredEquiv));
        lookupResolver.store(entry(filtered, ImmutableSet.of(filteredEquiv),
            subject, equiv, filteredEquiv));
        contentResolver.respondTo(ImmutableSet.of(subject, equiv, filtered, filteredEquiv));
        
        EquivalentContent content = equivResolver.resolveUris(uris, application, annotations, false);
        
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
        Set<Annotation> annotations = Annotation.defaultAnnotations();

        when(application.getConfiguration()).thenReturn(configWithSources(Publisher.PA, Publisher.BBC, Publisher.C4));

        lookupResolver.store(entry(subject, ImmutableSet.of(equiv), 
                equiv, filtered, filteredEquiv));
        lookupResolver.store(entry(equiv, ImmutableSet.of(subject), 
                subject, filtered, filteredEquiv));
        lookupResolver.store(entry(filtered, ImmutableSet.of(filteredEquiv),
                subject, equiv, filteredEquiv));
        lookupResolver.store(entry(filteredEquiv, ImmutableSet.of(filtered),
                subject, equiv, filtered));
        contentResolver.respondTo(ImmutableSet.of(subject, equiv, filtered, filteredEquiv));
        
        EquivalentContent content = equivResolver.resolveUris(uris, application, annotations, false);
        
        assertEquals(2, content.asMap().size());
        assertNull(content.asMap().get(subject.getCanonicalUri()));
        assertNull(content.asMap().get(filtered.getCanonicalUri()));
        
        checkEquivContent(content, equiv, filtered, filteredEquiv);
        checkEquivContent(content, filteredEquiv, filtered);
    }
    
    @Test
    public void testSubjectAlwaysKeepsItsAdjacents() {
        
        Episode subject = episode("subject", 4, Publisher.PA);
        Episode disabled = episode("disabled", 5, Publisher.FIVE);
        Episode equiv = episode("equiv", 2, Publisher.BBC);
        Episode filtered = episode("filtered", 1, Publisher.PA);
        
        Iterable<String> uris = ImmutableSet.of(subject.getCanonicalUri());
        Set<Annotation> annotations = Annotation.defaultAnnotations();

        when(application.getConfiguration()).thenReturn(configWithSources(Publisher.PA, Publisher.BBC, Publisher.C4));

        lookupResolver.store(entry(subject, ImmutableSet.of(equiv, disabled), 
            equiv, filtered, disabled));
        lookupResolver.store(entry(filtered, ImmutableSet.of(equiv),
            subject, equiv, disabled));
        contentResolver.respondTo(ImmutableSet.of(subject, equiv, filtered,disabled));
        
        EquivalentContent content = equivResolver.resolveUris(uris, application, annotations, false);
        
        assertEquals(1, content.asMap().size());
        assertNull(content.asMap().get(equiv.getCanonicalUri()));
        
        checkEquivContent(content, subject, equiv);
    }

    @Test
    public void annotationIgnoresReadableTransativeEquivsIndirectlyFromNonReadableEquivs() {
        DefaultEquivalentContentResolver resolver = (DefaultEquivalentContentResolver) equivResolver;

        when(application.getConfiguration()).thenReturn(configWithSources(Publisher.BARB_OVERRIDES, Publisher.BARB_MASTER, Publisher.BARB_TRANSMISSIONS));

        Predicate<LookupRef> sourceFilter = MorePredicates.transformingPredicate(
                LookupRef.TO_SOURCE,
                Predicates.in(application.getConfiguration().getEnabledReadSources()))::apply;

        Episode subject = episode("e1", 1, Publisher.BARB_OVERRIDES);
        Episode expEquivRead = episode("e2", 2, Publisher.BARB_MASTER);
        Episode expEquivReadTransRead = episode("e2-1", 3, Publisher.BARB_TRANSMISSIONS);
        Episode expEquivReadTransNoRead = episode("e2-3", 4, Publisher.BARB_X_MASTER);
        Episode expEquivNoRead = episode("e3", 5, Publisher.BARB_X_MASTER);
        Episode expEquivNoReadTransRead = episode("e3-1", 6, Publisher.BARB_TRANSMISSIONS);
        Episode expEquivNoReadTransNoRead = episode("e3-2", 7, Publisher.BARB_X_MASTER);

        LookupEntry subjEntry = entry(subject, expEquivNoRead, expEquivRead);
        lookupResolver.store(subjEntry);
        lookupResolver.store(entry(expEquivRead, expEquivReadTransNoRead, expEquivReadTransRead));
        lookupResolver.store(entry(expEquivNoReadTransRead, expEquivNoReadTransNoRead, expEquivNoReadTransRead));
        lookupResolver.store(entry(expEquivReadTransRead));

        Set<LookupRef> processedRefs = resolver.getEquivSetByFollowingLinks(subjEntry, sourceFilter);

        assertThat(processedRefs.size(), is(3));
        assertTrue(processedRefs.contains(LookupRef.from(subject)));
        assertTrue(processedRefs.contains(LookupRef.from(expEquivRead)));
        assertTrue(processedRefs.contains(LookupRef.from(expEquivReadTransRead)));
    }

    @Test
    public void annotationIgnoresUnpublishedEquivs() {
        DefaultEquivalentContentResolver resolver = (DefaultEquivalentContentResolver) equivResolver;

        when(application.getConfiguration()).thenReturn(configWithSources(Publisher.BARB_OVERRIDES, Publisher.BARB_MASTER, Publisher.BARB_TRANSMISSIONS));

        Predicate<LookupRef> sourceFilter = MorePredicates.transformingPredicate(
                LookupRef.TO_SOURCE,
                Predicates.in(application.getConfiguration().getEnabledReadSources()))::apply;

        Episode subject = episode("e1", 1, Publisher.BARB_OVERRIDES);
        Episode expEquiv = episode("e1-1", 2, Publisher.BARB_MASTER);
        Episode expEquivChild = episode("e1-1-1", 3, Publisher.BARB_TRANSMISSIONS);
        Episode expEquivUnpublished= episode("e1-2", 5, Publisher.BARB_MASTER);
        Episode expEquivChildOfUnpublished = episode("e1-2-1", 6, Publisher.BARB_TRANSMISSIONS);
        expEquivUnpublished.setActivelyPublished(false);

        LookupEntry subjEntry = entry(subject, expEquiv, expEquivUnpublished);
        lookupResolver.store(subjEntry);
        lookupResolver.store(entry(expEquiv, expEquivChild));
        lookupResolver.store(entry(expEquivUnpublished, expEquivChildOfUnpublished));
        lookupResolver.store(entry(expEquivChild));
        lookupResolver.store(entry(expEquivChildOfUnpublished));

        Set<LookupRef> processedRefs = resolver.getEquivSetByFollowingLinks(subjEntry, sourceFilter);

        assertThat(processedRefs.size(), is(3));
        assertTrue(processedRefs.contains(LookupRef.from(subject)));
        assertTrue(processedRefs.contains(LookupRef.from(expEquiv)));
        assertTrue(processedRefs.contains(LookupRef.from(expEquivChild)));
    }

    @Test
    public void annotationOnlyReturnsSingleContentIfContentUnpublished() {
        DefaultEquivalentContentResolver resolver = (DefaultEquivalentContentResolver) equivResolver;

        when(application.getConfiguration()).thenReturn(configWithSources(Publisher.BARB_OVERRIDES, Publisher.BARB_MASTER, Publisher.BARB_TRANSMISSIONS));

        Predicate<LookupRef> sourceFilter = MorePredicates.transformingPredicate(
                LookupRef.TO_SOURCE,
                Predicates.in(application.getConfiguration().getEnabledReadSources()))::apply;

        Episode subject = episode("e1", 1, Publisher.BARB_OVERRIDES);
        Episode expEquiv = episode("e1-1", 2, Publisher.BARB_MASTER);
        Episode expEquivChild = episode("e1-1-1", 3, Publisher.BARB_TRANSMISSIONS);
        subject.setActivelyPublished(false);

        LookupEntry subjEntry = entry(subject, expEquiv, expEquivChild);
        lookupResolver.store(subjEntry);
        lookupResolver.store(entry(expEquiv, expEquivChild));
        lookupResolver.store(entry(expEquivChild));

        Set<LookupRef> processedRefs = resolver.getEquivSetByFollowingLinks(subjEntry, sourceFilter);

        assertThat(processedRefs.size(), is(0));
    }

    private ApplicationConfiguration configWithSources(Publisher... srcs) {
        return ApplicationConfiguration.builder()
                .withPrecedence(Arrays.asList(srcs))
                .withEnabledWriteSources(ImmutableList.of())
                .build();
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
        EquivRefs equivRefs = toEquivRefs(direct);
        Set<LookupRef> transitiveSet = Arrays.stream(equivs)
                .map(LookupRef::from)
                .collect(MoreCollectors.toImmutableSet());
        return LookupEntry.lookupEntryFrom(c)
            .copyWithDirectEquivalents(equivRefs)
            .copyWithEquivalents(transitiveSet);
    }

    private LookupEntry entry(Content c, Content... equivs) {
        return LookupEntry.lookupEntryFrom(c)
                .copyWithExplicitEquivalents(toEquivRefs(ImmutableSet.copyOf(equivs)));
    }

    private EquivRefs toEquivRefs(Iterable<? extends Content> contents) {
        ImmutableMap.Builder<LookupRef, EquivRefs.Direction> equivRefs = ImmutableMap.builder();
        for (Content content : contents) {
            equivRefs.put(LookupRef.from(content), OUTGOING);
        }
        return EquivRefs.of(equivRefs.build());
    }
    
    private Episode episode(String uri, long id, Publisher src) {
        Episode ep = new Episode(uri, uri, src);
        ep.setId(id);
        return ep;
    }
    
}
