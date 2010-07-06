package org.uriplay.persistence.content;

import java.util.Collections;
import java.util.Set;

import org.uriplay.media.entity.Brand;
import org.uriplay.media.entity.Content;
import org.uriplay.media.entity.Item;
import org.uriplay.media.entity.Playlist;
import org.uriplay.persistence.content.ContentListener.changeType;

import com.google.common.collect.Sets;

public class EventFiringContentWriter implements ContentWriter {

	private final ContentWriter delegate;
	private final ContentListener listener;
	
	public EventFiringContentWriter(ContentWriter delegate, ContentListener listener) {
		this.delegate = delegate;
		this.listener = listener;
	}

	public void addAliases(String uri, Set<String> aliases) {
		delegate.addAliases(uri, aliases);
	}

	@Override
	public void createOrUpdateContent(Content root, boolean markMissingItemsAsUnavailable) {
        if (root instanceof Playlist) {
            createOrUpdatePlaylist((Playlist) root, markMissingItemsAsUnavailable);
        }
        if (root instanceof Item) {
            createOrUpdateItem((Item) root);
        }
	}
	
	@Override
	public void createOrUpdatePlaylist(Playlist enclosingList, boolean markMissingItemsAsUnavailable) {
		delegate.createOrUpdatePlaylist(enclosingList, markMissingItemsAsUnavailable);
		
		for (Playlist playlist : enclosingList.getPlaylists()) {
			listener.itemChanged(playlist.getItems(), changeType.CONTENT_UPDATE);
		}
		listener.itemChanged(enclosingList.getItems(), changeType.CONTENT_UPDATE);
		
		Set<Brand> brands = Sets.newHashSet();
		for (Playlist playlist : enclosingList.getPlaylists()) {
			if (playlist instanceof Brand) {
				brands.add((Brand) playlist);
			}
		}
		listener.brandChanged(brands, changeType.CONTENT_UPDATE);
	}
	
	@Override
	public void createOrUpdateItem(Item item) {
		delegate.createOrUpdateItem(item);
		listener.itemChanged(Collections.singletonList(item), changeType.CONTENT_UPDATE);
	}
}
