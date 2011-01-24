package org.atlasapi.persistence.content;

import java.util.List;

import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Playlist;

public interface RetrospectiveContentLister {

	List<Item> listItems(String fromId, int batchSize);
	
	List<Playlist> listPlaylists(String fromId, int batchSize);
}
