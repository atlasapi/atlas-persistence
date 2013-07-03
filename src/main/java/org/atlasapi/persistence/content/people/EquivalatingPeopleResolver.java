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

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class EquivalatingPeopleResolver implements PeopleQueryResolver {

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
        return Optional.fromNullable(findOrMerge(uri, resolvePeople(configuration, entriesForUri(uri)), configuration));
    }

    @Override
    public Optional<Person> person(Long id, ApplicationConfiguration configuration) {
        return Optional.fromNullable(findOrMerge(id, resolvePeople(configuration, entriesForId(id)), configuration));
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
        
        ImmutableMap<String, LookupEntry> lookup = Maps.uniqueIndex(Iterables.filter(lookupEntries, new Predicate<LookupEntry>() {

            @Override
            public boolean apply(LookupEntry input) {
                return configuration.isEnabled(input.lookupRef().publisher());
            }
        }), LookupEntry.TO_ID);

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
