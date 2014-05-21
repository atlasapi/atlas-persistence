package org.atlasapi.persistence.service;

import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Service;

import com.google.common.base.Optional;


public interface ServiceResolver {

    Optional<Service> serviceFor(long id);
    Iterable<Service> servicesFor(Alias alias);
    
}
