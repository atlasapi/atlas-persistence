package org.atlasapi.persistence.content;

import java.util.List;

import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.ContentGroup;

public interface RetrospectiveContentLister {

	List<Content> listAllRoots(String fromId, int batchSize);

	List<ContentGroup> listAllContentGroups(String fromId, int batchSize);
	
}
