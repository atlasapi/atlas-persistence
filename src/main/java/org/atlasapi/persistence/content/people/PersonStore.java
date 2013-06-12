package org.atlasapi.persistence.content.people;

import org.atlasapi.persistence.content.PeopleLister;
import org.atlasapi.persistence.content.PeopleResolver;

public interface PersonStore extends PersonWriter, PeopleResolver, PeopleLister {

}
