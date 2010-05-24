package org.uriplay.persistence;

import java.util.Collections;
import java.util.Set;

import com.google.common.collect.Sets;


/**
 * Null implementation of {@link ReadableBeanStore}.
 * @author Robert Chatley
 */
public class NullBeanStore implements ReadableBeanStore {

	public void store(Set<? extends Object> beans) {
	}

    public Set<Object> getAllResources() {
        return Sets.newHashSet();
    }

	public Iterable<Object> find(String query, String param) {
		return Collections.emptySet();
	}

}
