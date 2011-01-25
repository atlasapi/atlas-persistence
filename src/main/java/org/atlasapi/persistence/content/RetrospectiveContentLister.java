package org.atlasapi.persistence.content;

import java.util.List;

import org.atlasapi.media.entity.Content;

public interface RetrospectiveContentLister {

	List<Content> listAllRoots(String fromId, int batchSize);

}
