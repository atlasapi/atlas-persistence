package org.atlasapi.persistence.equiv;

import org.atlasapi.media.entity.Equiv;

public interface EquivalentUrlStore extends EquivalentUrlFinder {

	void store(Equiv equiv);
	
}
