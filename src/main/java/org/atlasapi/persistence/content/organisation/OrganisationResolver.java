package org.atlasapi.persistence.content.organisation;

import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.Person;

import com.google.common.base.Optional;


public interface OrganisationResolver {

    Optional<Person> person(String uri);
    
    Optional<Person> person(Long id);
    
    Iterable<Person> people(Iterable<LookupRef> lookupRefs);
}
