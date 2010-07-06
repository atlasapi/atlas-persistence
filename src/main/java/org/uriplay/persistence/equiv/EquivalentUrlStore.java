package org.uriplay.persistence.equiv;

import org.uriplay.media.entity.Equiv;

public interface EquivalentUrlStore extends EquivalentUrlFinder {

	void store(Equiv equiv);
	
}
