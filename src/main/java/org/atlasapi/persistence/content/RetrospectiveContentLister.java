package org.atlasapi.persistence.content;

import java.util.List;

import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Playlist;

import com.metabroadcast.common.query.Selection;

public interface RetrospectiveContentLister {

	List<Item> listAllItems(Selection selection);
	
	List<Playlist> listAllPlaylists(Selection selection);
}
