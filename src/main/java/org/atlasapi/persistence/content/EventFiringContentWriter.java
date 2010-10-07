package org.atlasapi.persistence.content;

import java.util.Collections;
import java.util.Set;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Playlist;
import org.atlasapi.persistence.content.ContentListener.changeType;

import com.google.common.collect.Sets;

public class EventFiringContentWriter implements ContentWriter, DefinitiveContentWriter {

	private final ContentWriter delegate;
	private final ContentListener listener;
    private final DefinitiveContentWriter definitiveDelgate;
	
	public EventFiringContentWriter(ContentWriter delegate, DefinitiveContentWriter definitiveDelgate, ContentListener listener) {
		this.delegate = delegate;
        this.definitiveDelgate = definitiveDelgate;
		this.listener = listener;
	}
	
	@Override
	public void createOrUpdatePlaylist(Playlist enclosingList, boolean markMissingItemsAsUnavailable) {
		delegate.createOrUpdatePlaylist(enclosingList, markMissingItemsAsUnavailable);
		notifyListener(enclosingList);
	}
	
	@Override
	public void createOrUpdateItem(Item item) {
		delegate.createOrUpdateItem(item);
		notifyListener(item);
	}

    @Override
    public void createOrUpdateDefinitiveItem(Item item) {
        definitiveDelgate.createOrUpdateDefinitiveItem(item);
        notifyListener(item);
    }
    
	@Override
	public void createOrUpdatePlaylistSkeleton(Playlist playlist) {
		delegate.createOrUpdatePlaylistSkeleton(playlist);
	}

    @Override
    public void createOrUpdateDefinitivePlaylist(Playlist playlist) {
        definitiveDelgate.createOrUpdateDefinitivePlaylist(playlist);
        notifyListener(playlist);
    }
    
    private void notifyListener(Playlist enclosingList) {
        for (Playlist playlist : enclosingList.getPlaylists()) {
            listener.itemChanged(playlist.getItems(), changeType.CONTENT_UPDATE);
        }
        listener.itemChanged(enclosingList.getItems(), changeType.CONTENT_UPDATE);
        
        Set<Brand> brands = Sets.newHashSet();
        if (enclosingList instanceof Brand) {
            brands.add((Brand) enclosingList);
        }
        for (Playlist playlist : enclosingList.getPlaylists()) {
            if (playlist instanceof Brand) {
                brands.add((Brand) playlist);
            }
        }
        listener.brandChanged(brands, changeType.CONTENT_UPDATE);
    }
    
    private void notifyListener(Item item) {
        listener.itemChanged(Collections.singletonList(item), changeType.CONTENT_UPDATE);
    }


}
