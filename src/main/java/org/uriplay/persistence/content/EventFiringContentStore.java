package org.uriplay.persistence.content;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.uriplay.media.entity.Brand;
import org.uriplay.media.entity.Description;
import org.uriplay.media.entity.Item;
import org.uriplay.media.entity.Playlist;
import org.uriplay.media.util.ChildFinder;
import org.uriplay.persistence.content.ContentListener.changeType;

import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.soy.common.collect.Sets;
import com.metabroadcast.common.query.Selection;

public class EventFiringContentStore implements MutableContentStore {

	private final MutableContentStore delegate;
	private final ContentListener listener;
	
	public EventFiringContentStore(MutableContentStore delegate, ContentListener listener) {
		this.delegate = delegate;
		this.listener = listener;
	}

	public void addAliases(String uri, Set<String> aliases) {
		delegate.addAliases(uri, aliases);
	}

	public void createOrUpdateGraph(Set<?> beans, boolean markMissingItemsAsUnavailable) {
        for (Object root : rootsOf(beans)) {
            if (root instanceof Playlist) {
                createOrUpdatePlaylist((Playlist) root, markMissingItemsAsUnavailable);
            }
            if (root instanceof Item) {
                createOrUpdateItem((Item) root);
            }
        }
	}
	
    private Iterable<?> rootsOf(Set<?> beans) {
        return Iterables.filter(beans, Predicates.not(new ChildFinder(beans)));
    }

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
	
	public void createOrUpdateItem(Item item) {
		delegate.createOrUpdateItem(item);
		listener.itemChanged(Collections.singletonList(item), changeType.CONTENT_UPDATE);
	}

	public Description findByUri(String uri) {
		return delegate.findByUri(uri);
	}

	public List<Item> findItems(List<String> uri) {
		return delegate.findItems(uri);
	}

	@Override
	public List<Playlist> findPlaylists(List<String> uri) {
		return delegate.findPlaylists(uri);
	}

    @Override
    public List<Item> listAllItems(Selection selection) {
        return delegate.listAllItems(selection);
    }

    @Override
    public List<Playlist> listAllPlaylists(Selection selection) {
        return delegate.listAllPlaylists(selection);
    }
}
