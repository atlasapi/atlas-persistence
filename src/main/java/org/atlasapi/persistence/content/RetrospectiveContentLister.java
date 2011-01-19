package org.atlasapi.persistence.content;

import java.util.Iterator;

import org.atlasapi.media.entity.Content;

public interface RetrospectiveContentLister {

	Iterator<Content> listAllRoots();
}
