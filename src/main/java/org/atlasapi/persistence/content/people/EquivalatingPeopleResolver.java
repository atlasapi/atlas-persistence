package org.atlasapi.persistence.content.people;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.metabroadcast.applications.client.model.internal.Application;
import org.atlasapi.equiv.OutputContentMerger;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.Person;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.PeopleQueryResolver;
import org.atlasapi.persistence.content.PeopleResolver;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Maps;
import com.metabroadcast.common.base.MorePredicates;
import com.metabroadcast.common.query.Selection;

public class EquivalatingPeopleResolver implements PeopleQueryResolver {

    private static final Person DEFAULTING_TO_NULL = null;
    
    private final PeopleResolver peopleResolver;
    private final LookupEntryStore peopleLookupEntryStore;
    private final OutputContentMerger outputContentMerger;
    
    public EquivalatingPeopleResolver(PeopleResolver peopleResolver, LookupEntryStore peopleLookupEntryStore) {
        this.peopleResolver = peopleResolver;
        this.peopleLookupEntryStore = peopleLookupEntryStore;
        this.outputContentMerger = new OutputContentMerger();
    }
    
    @Override
    public Optional<Person> person(String uri, Application application) {
        List<Person> people = people(ImmutableList.of(uri), application);
        return Optional.fromNullable(Iterables.getOnlyElement(people, null));
    }

    @Override
    public Optional<Person> person(Long id, Application application) {
        List<Person> people = peopleByIds(ImmutableList.of(id), application);
        return Optional.fromNullable(Iterables.getOnlyElement(people, null));
    }
    
    @Override
    public Iterable<Person> people(Iterable<Publisher> publishers, Application application,
            Selection selection) {
        
        Iterable<LookupEntry> entries = peopleLookupEntryStore.entriesForPublishers(publishers, selection);
        Map<String, LookupEntry> entriesIndex = Maps.uniqueIndex(entries, Functions.compose(LookupRef.TO_URI, LookupEntry.TO_SELF));
        
        Map<String, Person> peopleIndex = Maps.uniqueIndex(peopleForEntries(entriesIndex, application), Identified.TO_URI);
        
        ListMultimap<String, Person> idToPeople = keysToPeople(entriesIndex, peopleIndex, LookupRef.TO_URI);
        return findOrMerge(peopleIndex.keySet(), idToPeople, Identified.TO_URI, application);
    }
    
    @Override
    /*
     * Find people for URIs by resolving entries for those URIs to createDefault an
     * entry index. 
     * All required people are resolved from the set of all
     * equivalents of the resolved entries, creating an index of required
     * people. 
     * The requested URIs are transformed to people equivalence sets and
     * merged as necessary.
     */
    public List<Person> people(Iterable<String> uris, final Application application) {

        Iterable<LookupEntry> entries = peopleLookupEntryStore.entriesForIdentifiers(uris, true);
        Map<String, LookupEntry> entriesIndex = Maps.uniqueIndex(entries, LookupEntry.TO_ID);
        
        Map<String, Person> peopleIndex = Maps.uniqueIndex(peopleForEntries(entriesIndex, application), Identified.TO_URI);
        
        ListMultimap<String, Person> urisToPeople = keysToPeople(entriesIndex, peopleIndex, LookupRef.TO_URI);
        return findOrMerge(uris, urisToPeople, Identified.TO_URI, application);
    }
    

    public List<Person> peopleByIds(Iterable<Long> ids, final Application application) {
        
        Iterable<LookupEntry> entries = peopleLookupEntryStore.entriesForIds(ids);
        Map<Long, LookupEntry> entriesIndex = Maps.uniqueIndex(entries, Functions.compose(LookupRef.TO_ID, LookupEntry.TO_SELF));
        
        Map<Long, Person> peopleIndex = Maps.uniqueIndex(peopleForEntries(entriesIndex, application), Identified.TO_ID);
        
        ListMultimap<Long, Person> idToPeople = keysToPeople(entriesIndex, peopleIndex, LookupRef.TO_ID);
        return findOrMerge(ids, idToPeople, Identified.TO_ID, application);
    }

    private <T> List<Person> findOrMerge(Iterable<T> keys, ListMultimap<T, Person> keyToPeople,
            Function<? super Person, T> personToKey, Application application) {
        return application.getConfiguration().isPrecedenceEnabled() ? merge(keys, keyToPeople, application)
                                          : find(keys, keyToPeople, personToKey);
    }
    
    private <T> List<Person> merge(Iterable<T> keys, ListMultimap<T, Person> keyToPeople, Application application) {
        Builder<Person> result = ImmutableList.builder();
        for (T key : keys) {
            List<Person> people = keyToPeople.get(key);
            Person person = merge(people, application);
            if (person != null) {
                result.add(person);
            }
        }
        return result.build();
    }
    
    private <T> List<Person> find(Iterable<T> keys, ListMultimap<T, Person> keyToPeople, Function<? super Person, T> personToKey) {
        Builder<Person> result = ImmutableList.builder();
        for (T key : keys) {
            List<Person> people = keyToPeople.get(key);
            Person person = find(key, people, personToKey);
            if (person != null) {
                result.add(person);
            }
        }
        return result.build();
    }
    
    private <T> Iterable<Person> peopleForEntries(Map<T, LookupEntry> entriesIndex,
            Application application) {
        if (entriesIndex.isEmpty()) {
            return ImmutableList.of();
        }
        Iterable<Set<LookupRef>> entryRefs = Iterables.transform(entriesIndex.values(),LookupEntry.TO_EQUIVS);
        Predicate<LookupRef> sourceFilter = MorePredicates.transformingPredicate(LookupRef.TO_SOURCE, Predicates.in(application.getConfiguration().getEnabledReadSources()));
        Iterable<LookupRef> enabledRefs = Iterables.filter(Iterables.concat(entryRefs), sourceFilter);
        return peopleResolver.people(enabledRefs);
    }

    private <T> ListMultimap<T,Person> keysToPeople(Map<T, LookupEntry> entriesIndex,
            Map<T, Person> peopleIndex, Function<LookupRef, T> refToKey) {
        
        Function<LookupRef, Person> refToPerson = Functions.compose(Functions.forMap(peopleIndex, DEFAULTING_TO_NULL), refToKey);
        
        ImmutableListMultimap.Builder<T,Person> result = ImmutableListMultimap.builder();
        for (Entry<T, LookupEntry> entryMapping : entriesIndex.entrySet()) {
            Set<LookupRef> equivs = entryMapping.getValue().equivalents();
            Iterable<Person> people = Iterables.filter(Iterables.transform(equivs, refToPerson), Predicates.notNull());
            result.putAll(entryMapping.getKey(), setEquivalentToFields(ImmutableList.copyOf(people), equivs));
        }
        return result.build();
    }

    private Person merge(List<Person> people, Application application) {
        if (people == null || people.isEmpty()) {
            return null;
        }
        return Iterables.getFirst(outputContentMerger.merge(application, people), null);
    }

    private <T> Person find(T key, List<Person> people, Function<? super Person, T> personToKey) {
        if (people == null || people.isEmpty()) {
            return null;
        }
        for (Person person : people) {
            if (key.equals(personToKey.apply(person))) {
                return person;
            }
        }
        return null;
    }
    
    private List<Person> setEquivalentToFields(List<Person> people, Set<LookupRef> equivs) {
        for (Person person : people) {
            person.setEquivalentTo(equivs);
        }
        return people;
    }

}
