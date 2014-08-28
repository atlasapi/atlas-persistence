package org.atlasapi.persistence.content.organisation;

import static com.google.common.base.Preconditions.checkNotNull;

import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.Organisation;

import com.google.common.base.Optional;
import com.metabroadcast.common.ids.IdGenerator;


public class IdSettingOrganisationStore implements OrganisationStore {

    private final OrganisationStore delegate;
    private final IdGenerator idGenerator;
    
    public IdSettingOrganisationStore(OrganisationStore delegate, IdGenerator idGenerator) {
        this.delegate = checkNotNull(delegate);
        this.idGenerator = checkNotNull(idGenerator);
    }
    
    @Override
    public void updateOrganisationItems(Organisation organisation) {
        delegate.updateOrganisationItems(generateOrRestoreId(organisation));
    }

    @Override
    public void createOrUpdateOrganisation(Organisation organisation) {
        delegate.createOrUpdateOrganisation(generateOrRestoreId(organisation));
    }

    @Override
    public Optional<Organisation> organisation(String uri) {
        return delegate.organisation(uri);
    }

    @Override
    public Optional<Organisation> organisation(Long id) {
        return delegate.organisation(id);
    }

    @Override
    public Iterable<Organisation> organisations(Iterable<LookupRef> lookupRefs) {
        return delegate.organisations(lookupRefs);
    }
    
    private Organisation generateOrRestoreId(Organisation organisation) {
        Optional<Organisation> existing = organisation(organisation.getCanonicalUri());
        if (existing.isPresent() && existing.get().getId() != null) {
            organisation.setId(existing.get().getId());
        } else {
            organisation.setId(idGenerator.generateRaw());
        }
        return organisation;
    }

}
