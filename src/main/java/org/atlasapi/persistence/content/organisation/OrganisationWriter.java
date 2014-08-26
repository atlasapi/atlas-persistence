package org.atlasapi.persistence.content.organisation;

import org.atlasapi.media.entity.Organisation;


public interface OrganisationWriter {

    void updateOrganisationItems(Organisation organisation);
    
    void createOrUpdateOrganisation(Organisation organisation);
}
