package org.atlasapi.persistence.application;

import org.atlasapi.application.Application;
import org.atlasapi.media.common.Id;
import org.atlasapi.media.common.IdResolver;
import org.atlasapi.media.entity.Publisher;

import com.google.common.base.Optional;

public interface ApplicationStore extends IdResolver<Application> {

    Iterable<Application> allApplications();

    Optional<Application> applicationFor(Id id);
    
    void createApplication(Application application);

    void updateApplication(Application application);

    Iterable<Application> applicationsFor(Iterable<Id> ids);
    
    Iterable<Application> readersFor(Publisher source);
    
    Iterable<Application> writersFor(Publisher source);
}
