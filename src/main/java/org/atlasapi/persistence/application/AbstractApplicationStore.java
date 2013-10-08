package org.atlasapi.persistence.application;

import org.atlasapi.application.Application;
import org.atlasapi.application.ApplicationCredentials;
import org.atlasapi.application.ApplicationSources;
import org.atlasapi.media.common.Id;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.metabroadcast.common.ids.IdGenerator;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.UUIDGenerator;
import com.metabroadcast.common.time.SystemClock;

public abstract class AbstractApplicationStore implements ApplicationStore {
    private final IdGenerator idGenerator;
    private final NumberToShortStringCodec idCodec;
    private final SystemClock clock;

    public AbstractApplicationStore(IdGenerator idGenerator, 
            NumberToShortStringCodec idCodec, SystemClock clock) {
        this.idGenerator = idGenerator;
        this.idCodec = idCodec;
        this.clock = clock;
    }
    
    public AbstractApplicationStore(IdGenerator idGenerator, 
            NumberToShortStringCodec idCodec) {
        this(idGenerator, idCodec, new SystemClock());
    }
    
    // For compatibility with 3.0
    private String generateSlug(Id id) {
        return "app-" + idCodec.encode(id.toBigInteger());
    }
    
    private String generateApiKey() {
        return new UUIDGenerator().generate();
    }

    abstract void doCreateApplication(Application application);

    abstract void doUpdateApplication(Application application);
    
    public Application createApplication(Application application) {
        // Create requests do not have to post credentials or 
        // sources part of the object so ensure these exist
        ApplicationCredentials.Builder credentialsBuilder;
        if (application.getCredentials() != null) {
            credentialsBuilder = application.getCredentials().copy();
        } else {
            credentialsBuilder = ApplicationCredentials.builder();
        }
        credentialsBuilder = credentialsBuilder.withApiKey(generateApiKey());
        ApplicationSources sources = application.getSources();
        // If sources not given create an empty sources object
        if (sources == null) {
            sources = ApplicationSources.DEFAULT_SOURCES;
        }
        
        Id id = Id.valueOf(idGenerator.generateRaw());
        Application created = application.copy()
                .withId(id)
                .withCreated(clock.now())
                .withSlug(generateSlug(id))
                .withCredentials(credentialsBuilder.build())
                .withSources(sources)
                .build();
        
        doCreateApplication(created);
        return created;
    }

    @Override
    public Application updateApplication(Application application) {
        Application updated = withGuaranteedSlug(application);
        doUpdateApplication(updated);
        return updated;
    }
    
    private Application withGuaranteedSlug(Application application) {
        Preconditions.checkNotNull(application);
        // Ensure slug is present for compatibility with 3.0
        if (Strings.isNullOrEmpty(application.getSlug())) {
            application = application.copy()
                    .withSlug(generateSlug(application.getId())).build();
        }
        return application;
    }
  
}
