package org.atlasapi.persistence.content;

import java.util.Iterator;

import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Playlist;

public interface RetrospectiveContentLister {

	Iterator<Item> listAllItems();
	
	Iterator<Playlist> listAllPlaylists();
}
