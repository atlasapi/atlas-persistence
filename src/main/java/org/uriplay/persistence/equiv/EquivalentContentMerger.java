package org.uriplay.persistence.equiv;

import java.util.List;

import org.uriplay.media.entity.Brand;
import org.uriplay.media.entity.Content;
import org.uriplay.media.entity.Item;
import org.uriplay.media.entity.Playlist;

import com.google.common.collect.Lists;

public class EquivalentContentMerger {

	private final EquivalentContentFinder finder;

	public EquivalentContentMerger(EquivalentContentFinder finder) {
		this.finder = finder;
	}
	
	public Item merge(Item item) {
		// for now don't merge items directly
		return item;
	}
	
	public Playlist merge(Playlist playlist) {
		if (playlist instanceof Brand) {
			return merge((Brand) playlist);
		} else {
			List<Playlist> mergedPlaylists = Lists.newArrayList();
			for (Playlist subPlaylist : playlist.getPlaylists()) {
				mergedPlaylists.add(merge(subPlaylist));
			}
			playlist.setPlaylists(mergedPlaylists);
			return playlist;
		}
	}

	private Brand merge(Brand brand) {
		Iterable<? extends Content> equiv = finder.equivalentTo(brand);
		for (Content content : equiv) {
			if (content instanceof Brand) {
				mergeSubItemsOf(brand, (Brand) content);
			}
		}
		return brand;
	}

	private void mergeSubItemsOf(Brand original, Brand equivTo) {
		// TODO
	}
}
