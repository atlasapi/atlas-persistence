package org.atlasapi.persistence.content.people;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.atlasapi.application.ApplicationConfiguration;
import org.atlasapi.equiv.OutputContentMerger;
import org.atlasapi.media.entity.Described;
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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.metabroadcast.common.base.MorePredicates;

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
    public Optional<Person> person(String uri, ApplicationConfiguration configuration) {
        List<Person> people = people(ImmutableList.of(uri), configuration);
        return Optional.fromNullable(Iterables.getOnlyElement(people, null));
    }

    @Override
    public Optional<Person> person(Long id, ApplicationConfiguration configuration) {
        return Optional.fromNullable(findOrMerge(id, resolvePeople(configuration, entriesForId(id)), configuration));
    }
    
    @Override
    /*
     * Find people for URIs by resolving entries for those URIs to create an
     * entry index. 
     * All required people are resolved from the set of all
     * equivalents of the resolved entries, creating an index of required
     * people. 
     * The requested uris are transformed to people equivalence sets and
     * merged as necessary.
     */
    public List<Person> people(Iterable<String> uris, final ApplicationConfiguration config) {

        Map<String, LookupEntry> entriesIndex = entriesForUris(uris);
        if (entriesIndex.isEmpty()) {
            return ImmutableList.of();
        }
        
        Predicate<LookupRef> sourceFilter = MorePredicates.transformingPredicate(LookupRef.TO_SOURCE, Predicates.in(config.getEnabledSources()));
        Map<String, Person> peopleIndex = peopleForEntryEnabledEquivs(entriesIndex.values(), sourceFilter);
        
        return urisToPeople(uris, entriesIndex, peopleIndex, config);
    }

    private List<Person> urisToPeople(Iterable<String> uris,
            Map<String, LookupEntry> entriesIndex, Map<String, Person> peopleIndex,
            ApplicationConfiguration config) {
        
        Function<LookupRef, Person> refToPerson = Functions.compose(Functions.forMap(peopleIndex, DEFAULTING_TO_NULL), LookupRef.TO_URI);
        
        ImmutableList.Builder<Person> result = ImmutableList.builder();
        for (String uri : uris) {
            LookupEntry entry = entriesIndex.get(uri);
            if (entry == null) {
                // entries may not be in the index because the
                // people don't exist or because they're from
                // disabled sources.
                continue;
            }
            Set<LookupRef> equivs = entry.equivalents();
            Iterable<Person> people = Iterables.filter(Iterables.transform(equivs, refToPerson), Predicates.notNull());
            List<Person> equivPeople = setEquivalentToFields(ImmutableList.copyOf(people));
            Person person = findOrMerge(uri, equivPeople, config);
            if (person != null) {
                result.add(person);
            }
        }
        return result.build();
    }

    private Map<String, Person> peopleForEntryEnabledEquivs(Iterable<LookupEntry> entries, Predicate<LookupRef> sourceFilter) {
        Iterable<Set<LookupRef>> entryRefs = Iterables.transform(entries,LookupEntry.TO_EQUIVS);
        Iterable<LookupRef> enabledRefs = Iterables.filter(Iterables.concat(entryRefs), sourceFilter);
        if (Iterables.isEmpty(enabledRefs)) {
            return ImmutableMap.of();
        }
        Iterable<Person> people = peopleResolver.people(enabledRefs);
        return Maps.uniqueIndex(people, Identified.TO_URI);
    }

    private Map<String, LookupEntry> entriesForUris(Iterable<String> uris) {
        Iterable<LookupEntry> entries = peopleLookupEntryStore.entriesForIdentifiers(uris, true);
        return Maps.uniqueIndex(entries, LookupEntry.TO_ID);
    }

    private Person findOrMerge(String uri, List<Person> resolved, ApplicationConfiguration configuration) {
        return configuration.precedenceEnabled() ? merge(resolved, configuration)
                                                 : find(uri, resolved);
    }
    
    private Person findOrMerge(Long id, List<Person> resolved, ApplicationConfiguration configuration) {
        return configuration.precedenceEnabled() ? merge(resolved, configuration)
                                                 : find(id, resolved);
    }

    private Person merge(List<Person> resolved, ApplicationConfiguration configuration) {
        return Iterables.getFirst(outputContentMerger.merge(configuration, resolved), null);
    }

    private Person find(String uri, List<Person> resolved) {
        for (Person person : resolved) {
            if (uri.equals(person.getCanonicalUri())) {
                return person;
            }
        }
        return null;
    }
    
    private Person find(Long id, List<Person> resolved) {
        for (Person person : resolved) {
            if (id.equals(person.getId())) {
                return person;
            }
        }
        return null;
    }

    private Iterable<LookupEntry> entriesForId(Long id) {
        return peopleLookupEntryStore.entriesForIds(ImmutableList.of(id));
    }
    
    private Iterable<LookupEntry> entriesForUri(String uri) {
        return peopleLookupEntryStore.entriesForIdentifiers(ImmutableList.of(uri), true);
    }
    
    private List<Person> resolvePeople(final ApplicationConfiguration configuration, Iterable<LookupEntry> lookupEntries) {
        if(Iterables.isEmpty(lookupEntries)) {
            return ImmutableList.of();
        }
        
        ImmutableMap<String, LookupEntry> lookup = Maps.uniqueIndex(lookupEntries, LookupEntry.TO_ID);

        Map<String, Set<LookupRef>> lookupRefs = Maps.transformValues(lookup, LookupEntry.TO_EQUIVS);

        Iterable<LookupRef> filteredRefs = Iterables.filter(Iterables.concat(lookupRefs.values()), enabledPublishers(configuration));

        if (Iterables.isEmpty(filteredRefs)) {
            return ImmutableList.of();
        }

        return setEquivalentToFields(ImmutableList.copyOf(peopleResolver.people(filteredRefs)));
        
    }
    
    private List<Person> setEquivalentToFields(List<Person> resolvedResults) {
        Map<Described, LookupRef> equivRefs = Maps.newHashMap();
        for (Identified ided : resolvedResults) {
            if (ided instanceof Described) {
                Described described = (Described) ided;
                equivRefs.put(described, LookupRef.from(described));
            }
        }
        Set<LookupRef> lookupRefs = ImmutableSet.copyOf(equivRefs.values());
        for (Entry<Described, LookupRef> equivRef : equivRefs.entrySet()) {
            equivRef.getKey().setEquivalentTo(Sets.difference(lookupRefs, ImmutableSet.of(equivRef.getValue())));
        }
        return resolvedResults;
    }
    
    private Predicate<LookupRef> enabledPublishers(ApplicationConfiguration config) {
        final Set<Publisher> enabledPublishers = config.getEnabledSources();
        return new Predicate<LookupRef>() {

            @Override
            public boolean apply(LookupRef input) {
                return enabledPublishers.contains(input.publisher());
            }
        };
    }

}
