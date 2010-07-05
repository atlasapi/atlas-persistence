package org.uriplay.persistence.equiv;

import java.util.Set;

import org.uriplay.media.entity.Equiv;

public interface EquivStore {

	void store(Equiv equiv);

	Set<String> get(Iterable<String> ids);
	
}
