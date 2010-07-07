package org.atlasapi.persistence.equiv;

import java.util.Set;

public interface EquivalentUrlFinder {

	Set<String> get(Iterable<String> ids);

}
