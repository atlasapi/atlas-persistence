package org.atlasapi.persistence.content.people;

import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.atlasapi.application.ApplicationConfiguration;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.Person;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.PeopleResolver;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

public class EquivalatingPeopleResolverTest {
    
    private final PeopleResolver peopleResolver = mock(PeopleResolver.class);
    private final LookupEntryStore entryStore = mock(LookupEntryStore.class);
    private final EquivalatingPeopleResolver resolver
        = new EquivalatingPeopleResolver(peopleResolver, entryStore);
    
    @Test
    public void testResolvingPersonByUri() {

        Person primary = person(1L, "primary", Publisher.BBC);
        LookupEntry primaryEntry = LookupEntry.lookupEntryFrom(primary);
        
        when(entryStore.entriesForIdentifiers(argThat(hasItems(primary.getCanonicalUri())), anyBoolean()))
            .thenReturn(ImmutableList.of(primaryEntry));
        when(peopleResolver.people(argThat(hasItems(primaryEntry.lookupRef()))))
            .thenReturn(ImmutableList.of(primary));
        
        ApplicationConfiguration config = ApplicationConfiguration.defaultConfiguration();
        Optional<Person> person = resolver.person(primary.getCanonicalUri(), config);
        
        assertThat(person.get(), is(primary));
    }

    @Test
    public void testResolvingPersonWithEquivsByUriForDisabledSourceReturnsNothingWhenPrecedenceDisabled() {
        
        Person primary = person(1L, "primary", Publisher.PA);
        Person equiv = person(2L, "equiv", Publisher.BBC);

        LookupRef equivRef = LookupRef.from(equiv);
        LookupEntry primaryEntry = LookupEntry.lookupEntryFrom(primary)
                .copyWithEquivalents(ImmutableSet.of(equivRef));
        
        when(entryStore.entriesForIdentifiers(argThat(hasItems(primary.getCanonicalUri())), anyBoolean()))
            .thenReturn(ImmutableList.of(primaryEntry));
        when(peopleResolver.people(argThat(hasItems(equivRef))))
            .thenReturn(ImmutableList.of(equiv));
        
        ApplicationConfiguration config = ApplicationConfiguration.defaultConfiguration();
        Optional<Person> person = resolver.person(primary.getCanonicalUri(), config);
        
        assertFalse(person.isPresent());
    }

    @Test
    public void testResolvingPersonWithEquivsByUriForDisabledSourceReturnsEnabledEquivWhenPrecedenceEnabled() {
        
        Person primary = person(1L, "primary", Publisher.PA);
        Person equiv = person(2L, "equiv", Publisher.BBC);
        
        LookupRef equivRef = LookupRef.from(equiv);
        LookupEntry primaryEntry = LookupEntry.lookupEntryFrom(primary)
                .copyWithEquivalents(ImmutableSet.of(equivRef));
        
        when(entryStore.entriesForIdentifiers(argThat(hasItems(primary.getCanonicalUri())), anyBoolean()))
            .thenReturn(ImmutableList.of(primaryEntry));
        when(peopleResolver.people(argThat(hasItems(equivRef))))
            .thenReturn(ImmutableList.of(equiv));
        
        ApplicationConfiguration config = ApplicationConfiguration.defaultConfiguration()
                .copyWithPrecedence(ImmutableList.of(Publisher.BBC));
        Optional<Person> person = resolver.person(primary.getCanonicalUri(), config);
        
        assertTrue(person.isPresent());
        assertThat(person.get().getCanonicalUri(), is(equiv.getCanonicalUri()));
        assertThat(person.get().getEquivalentTo(), hasItems(primaryEntry.lookupRef(), equivRef));
    }

    @Test
    public void testResolvingPersonById() {
        
        Person primary = person(1L, "primary", Publisher.BBC);
        LookupEntry primaryEntry = LookupEntry.lookupEntryFrom(primary);
        
        when(entryStore.entriesForIds(argThat(hasItems(primary.getId()))))
            .thenReturn(ImmutableList.of(primaryEntry));
        when(peopleResolver.people(argThat(hasItems(primaryEntry.lookupRef()))))
            .thenReturn(ImmutableList.of(primary));
        
        ApplicationConfiguration config = ApplicationConfiguration.defaultConfiguration();
        Optional<Person> person = resolver.person(primary.getId(), config);
        
        assertThat(person.get(), is(primary));
    }

    private Person person(long id, String uri, Publisher source) {
        Person primary = new Person(uri,uri,source);
        primary.setId(id);
        return primary;
    }
    
    @Test
    public void testResolvingPersonWithEquivsByIdForDisabledSourceReturnsNothingWhenPrecedenceDisabled() {
        
        Person primary = person(1L, "primary", Publisher.PA);
        Person equiv = person(2L, "equiv", Publisher.BBC);

        LookupRef equivRef = LookupRef.from(equiv);
        LookupEntry primaryEntry = LookupEntry.lookupEntryFrom(primary)
                .copyWithEquivalents(ImmutableSet.of(equivRef));
        
        when(entryStore.entriesForIds(argThat(hasItems(primary.getId()))))
            .thenReturn(ImmutableList.of(primaryEntry));
        when(peopleResolver.people(argThat(hasItems(equivRef))))
            .thenReturn(ImmutableList.of(equiv));
        
        ApplicationConfiguration config = ApplicationConfiguration.defaultConfiguration();
        Optional<Person> person = resolver.person(primary.getId(), config);
        
        assertFalse(person.isPresent());
    }

    @Test
    public void testResolvingPersonWithEquivsByIdForDisabledSourceReturnsEnabledEquivWhenPrecedenceEnabled() {
        
        Person primary = person(1L, "primary", Publisher.PA);
        Person equiv = person(2L, "equiv", Publisher.BBC);
        
        LookupRef equivRef = LookupRef.from(equiv);
        LookupEntry primaryEntry = LookupEntry.lookupEntryFrom(primary)
                .copyWithEquivalents(ImmutableSet.of(equivRef));
        
        when(entryStore.entriesForIds(argThat(hasItems(primary.getId()))))
            .thenReturn(ImmutableList.of(primaryEntry));
        when(peopleResolver.people(argThat(hasItems(equivRef))))
            .thenReturn(ImmutableList.of(equiv));
        
        ApplicationConfiguration config = ApplicationConfiguration.defaultConfiguration()
                .copyWithPrecedence(ImmutableList.of(Publisher.BBC));
        Optional<Person> person = resolver.person(primary.getId(), config);
        
        assertTrue(person.isPresent());
        assertThat(person.get().getCanonicalUri(), is(equiv.getCanonicalUri()));
        assertThat(person.get().getEquivalentTo(), hasItems(primaryEntry.lookupRef(), equivRef));
    }
    
    @Test
    public void testResolvingPeople() {

        Person primary = person(1L, "primary", Publisher.BBC);
        Person equiv = person(2L, "equiv", Publisher.DBPEDIA);
        
        LookupEntry primaryEntry = LookupEntry.lookupEntryFrom(primary)
                .copyWithEquivalents(ImmutableSet.of(LookupRef.from(equiv)));
        
        when(entryStore.entriesForIdentifiers(argThat(hasItems(primary.getCanonicalUri())),anyBoolean()))
            .thenReturn(ImmutableList.of(primaryEntry));
        
        when(peopleResolver.people(argThat(hasItems(primaryEntry.lookupRef()))))
            .thenReturn(ImmutableList.of(primary, equiv));
        
        ApplicationConfiguration config = ApplicationConfiguration.defaultConfiguration();
        Iterable<Person> persons = resolver.people(ImmutableList.of(primary.getCanonicalUri()), config);
        
        Person person = Iterables.getOnlyElement(persons);
        assertThat(person, is(primary));
        assertThat(person.getEquivalentTo(), hasItems(primaryEntry.lookupRef(), LookupRef.from(equiv)));
    }
    
    @Test
    public void testResolvingPeopleWithPublisherNotEnabled() {

        Person primary = person(1L, "primary", Publisher.BBC);
        Person equiv = person(2L, "equiv", Publisher.PA);
        
        LookupRef equivRef = LookupRef.from(equiv);
        LookupEntry primaryEntry = LookupEntry.lookupEntryFrom(primary)
                .copyWithEquivalents(ImmutableSet.of(equivRef));
        
        when(entryStore.entriesForIdentifiers(argThat(hasItems(primary.getCanonicalUri())),anyBoolean()))
            .thenReturn(ImmutableList.of(primaryEntry));

        when(peopleResolver.people(argThat(hasItems(primaryEntry.lookupRef()))))
            .thenReturn(ImmutableList.of(primary));
        
        ApplicationConfiguration noPaConfig = ApplicationConfiguration.defaultConfiguration();
        Iterable<Person> persons = resolver.people(ImmutableList.of(primary.getCanonicalUri()), noPaConfig);
        
        Person person = Iterables.getOnlyElement(persons);
        assertThat(person, is(primary));
        assertThat(person.getEquivalentTo(), hasItems(primaryEntry.lookupRef(), equivRef));
    }
    
    @Test
    public void testResolvingPeopleReturnsEmptyWhenNoEntryFound() {
        
        String uriWithNoEntry = "uri";
        when(entryStore.entriesForIdentifiers(argThat(hasItems(uriWithNoEntry)),anyBoolean()))
            .thenReturn(ImmutableList.<LookupEntry>of());
        
        ApplicationConfiguration config = ApplicationConfiguration.defaultConfiguration();
        Iterable<Person> persons = resolver.people(ImmutableList.of(uriWithNoEntry), config);
      
        assertTrue(Iterables.isEmpty(persons));
        
        verify(peopleResolver, never()).people(anyLookupRefs());
        
    }

    @Test
    public void testResolvingPeopleReturnsEmptyWhenNoEntryFromDisabledSource() {
        
        Person disabledSourcePerson = person(1L,"primary",Publisher.PA);
        
        LookupEntry disabledSourceEntry = LookupEntry.lookupEntryFrom(disabledSourcePerson);
        
        when(entryStore.entriesForIdentifiers(argThat(hasItems(disabledSourcePerson.getCanonicalUri())),anyBoolean()))
            .thenReturn(ImmutableList.of(disabledSourceEntry));

        when(peopleResolver.people(anyLookupRefs()))
            .thenReturn(ImmutableList.<Person>of());
        
        ApplicationConfiguration config = ApplicationConfiguration.defaultConfiguration();
        Iterable<Person> persons = resolver.people(ImmutableList.of(disabledSourcePerson.getCanonicalUri()), config);
        
        assertTrue(Iterables.isEmpty(persons));
        
        ArgumentCaptor<Iterable<LookupRef>> refsCaptor = refsCaptor();
        verify(peopleResolver).people(refsCaptor.capture());
        
        assertTrue(Iterables.isEmpty(refsCaptor.getValue()));
        
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private ArgumentCaptor<Iterable<LookupRef>> refsCaptor() {
        return (ArgumentCaptor) ArgumentCaptor.forClass(Iterable.class);
    }
    
    @SuppressWarnings("unchecked")
    private Iterable<LookupRef> anyLookupRefs() {
        return argThat(any(Iterable.class));
    }
    
}
