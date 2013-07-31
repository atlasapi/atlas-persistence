package org.atlasapi.persistence.content.people;

import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

public class EquivalatingPeopleResolverTest {
    
    private final PeopleResolver peopleResolver = mock(PeopleResolver.class);
    private final LookupEntryStore entryStore = mock(LookupEntryStore.class);
    private final EquivalatingPeopleResolver resolver
        = new EquivalatingPeopleResolver(peopleResolver, entryStore);
    
    @Test
    public void testResolvingPeople() {
        
        Person primary = new Person("primary","primary",Publisher.BBC);
        Person equiv = new Person("equiv", "equiv", Publisher.DBPEDIA);
        
        LookupEntry primaryEntry = LookupEntry.lookupEntryFrom(primary)
                .copyWithEquivalents(ImmutableSet.of(LookupRef.from(equiv)));
        
        when(entryStore.entriesForCanonicalUris(argThat(hasItems(primary.getCanonicalUri()))))
            .thenReturn(ImmutableList.of(primaryEntry));
        
        when(peopleResolver.people(argThat(hasItems(primaryEntry.lookupRef()))))
            .thenReturn(ImmutableList.of(primary, equiv));
        
        ApplicationConfiguration config = ApplicationConfiguration.defaultConfiguration();
        Iterable<Person> persons = resolver.people(ImmutableList.of(primary.getCanonicalUri()), config);
        
        Person person = Iterables.getOnlyElement(persons);
        assertThat(person, is(primary));
        assertThat(person.getEquivalentTo(), hasItem(LookupRef.from(equiv)));
    }
    
    @Test
    public void testResolvingPeopleWithPublisherNotEnabled() {
        
        Person primary = new Person("primary","primary",Publisher.BBC);
        Person equiv = new Person("equiv", "equiv", Publisher.PA);
        
        LookupEntry primaryEntry = LookupEntry.lookupEntryFrom(primary)
                .copyWithEquivalents(ImmutableSet.of(LookupRef.from(equiv)));
        
        when(entryStore.entriesForCanonicalUris(argThat(hasItems(primary.getCanonicalUri()))))
            .thenReturn(ImmutableList.of(primaryEntry));

        when(peopleResolver.people(argThat(hasItems(primaryEntry.lookupRef()))))
            .thenReturn(ImmutableList.of(primary));
        
        ApplicationConfiguration noPaConfig = ApplicationConfiguration.defaultConfiguration();
        Iterable<Person> persons = resolver.people(ImmutableList.of(primary.getCanonicalUri()), noPaConfig);
        
        Person person = Iterables.getOnlyElement(persons);
        assertThat(person, is(primary));
        assertTrue(person.getEquivalentTo().isEmpty());
    }
    
    @Test
    public void testResolvingPeopleReturnsEmptyWhenNoEntryFound() {
        
        String uriWithNoEntry = "uri";
        when(entryStore.entriesForCanonicalUris(argThat(hasItems(uriWithNoEntry))))
            .thenReturn(ImmutableList.<LookupEntry>of());
        
        ApplicationConfiguration config = ApplicationConfiguration.defaultConfiguration();
        Iterable<Person> persons = resolver.people(ImmutableList.of(uriWithNoEntry), config);
      
        assertTrue(Iterables.isEmpty(persons));
        
        verify(peopleResolver, never()).people(anyLookupRefs());
        
    }

    @Test
    public void testResolvingPeopleReturnsEmptyWhenNoEntryFromDisabledSource() {
        
        Person disabledSourcePerson = new Person("primary","primary",Publisher.PA);
        
        LookupEntry disabledSourceEntry = LookupEntry.lookupEntryFrom(disabledSourcePerson);
        

        when(entryStore.entriesForCanonicalUris(argThat(hasItems(disabledSourcePerson.getCanonicalUri()))))
            .thenReturn(ImmutableList.of(disabledSourceEntry));
        
        ApplicationConfiguration config = ApplicationConfiguration.defaultConfiguration();
        Iterable<Person> persons = resolver.people(ImmutableList.of(disabledSourcePerson.getCanonicalUri()), config);
        
        assertTrue(Iterables.isEmpty(persons));
        
        verify(peopleResolver, never()).people(anyLookupRefs());
        
    }
    
    @SuppressWarnings("unchecked")
    private Iterable<LookupRef> anyLookupRefs() {
        return argThat(any(Iterable.class));
    }
    
}
