package org.uriplay.persistence.content;

import java.util.List;

import org.uriplay.media.entity.Item;
import org.uriplay.media.entity.Playlist;

import com.metabroadcast.common.query.Selection;

public interface RetrospectiveContentLister {

	List<Item> listAllItems(Selection selection);
	
	List<Playlist> listAllPlaylists(Selection selection);
}
