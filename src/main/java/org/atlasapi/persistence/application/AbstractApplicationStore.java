package org.atlasapi.persistence.application;

import org.atlasapi.application.Application;
import org.atlasapi.application.ApplicationCredentials;
import org.atlasapi.media.common.Id;

import com.google.common.base.Preconditions;
import com.metabroadcast.common.ids.IdGenerator;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.UUIDGenerator;

public abstract class AbstractApplicationStore implements ApplicationStore {
    private final IdGenerator idGenerator;
    private final NumberToShortStringCodec idCodec;

    public AbstractApplicationStore(IdGenerator idGenerator, 
            NumberToShortStringCodec idCodec) {
        this.idGenerator = idGenerator;
        this.idCodec = idCodec;
    }
    
    // For compatibility with 3.0
    private String generateSlug(Id id) {
        return "app-" + idCodec.encode(id.toBigInteger());
    }
    
    private String generateApiKey() {
        return new UUIDGenerator().generate();
    }
    
    protected Application addIdAndApiKey(Application application) {
        Id id = Id.valueOf(idGenerator.generateRaw());
        ApplicationCredentials credentials = application.getCredentials()
                .copy().withApiKey(generateApiKey()).build();
        Application modified = application.copy()
                .withId(id)
                .withCredentials(credentials)
                .withSlug(generateSlug(id))
                .build();
        return modified;
    }

    abstract void doCreateApplication(Application application);

    abstract void doUpdateApplication(Application application);
    
    public void createApplication(Application application) {
        doCreateApplication(ensureApplicationHasSlug(addIdAndApiKey(application)));
    }

    @Override
    public void updateApplication(Application application) {
        doUpdateApplication(ensureApplicationHasSlug(application));
    }
    
    private Application ensureApplicationHasSlug(Application application) {
        Preconditions.checkNotNull(application);
        // Ensure slug is present for compatibility with 3.0
        if (application.getSlug() == null || application.getSlug().isEmpty()) {
            application = application.copy()
                    .withSlug(generateSlug(application.getId())).build();
        }
        return application;
    }
  
}
