package org.atlasapi.persistence.content.organisation;

import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.Organisation;
import org.atlasapi.media.entity.Topic;

import com.google.common.base.Optional;


public interface OrganisationResolver {

    Optional<Organisation> organisation(String uri);
    
    Optional<Organisation> organisation(Long id);
    
    Iterable<Organisation> organisations(Iterable<LookupRef> lookupRefs);
    
    Iterable<Organisation> fetch(Optional<Topic> eventGroup);
}
