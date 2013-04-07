package org.atlasapi.persistence.content.people;

import org.atlasapi.persistence.content.PeopleListerListener;

public interface PeopleLister {

	void list(PeopleListerListener handler);
	
}
