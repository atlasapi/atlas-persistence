package org.uriplay.persistence.equiv;

import java.util.List;

import org.uriplay.media.entity.Brand;
import org.uriplay.media.entity.Content;
import org.uriplay.media.entity.Encoding;
import org.uriplay.media.entity.Episode;
import org.uriplay.media.entity.Item;
import org.uriplay.media.entity.Playlist;
import org.uriplay.media.entity.Version;

import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
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

	@SuppressWarnings("unchecked")
	private void mergeSubItemsOf(Brand original, Brand equivTo) {
		for (Item item : original.getItems()) {
			Version version = Iterables.get(item.getVersions(), 0, null);
			if (version == null) {
				continue;
			}
			Item sameItem = findSameItem((Episode) item, (List) equivTo.getItems());
			if (sameItem != null && !sameItem.getVersions().isEmpty()) {
				for (Encoding encoding : Iterables.get(sameItem.getVersions(), 0).getManifestedAs()) {
					version.addManifestedAs(encoding);
				}
			}
		}
	}

	private Item findSameItem(Episode needle, List<Episode> haystack) {
		for (Episode item : haystack) {
			if (areSame(needle, item)) {
				return item;
			}
		}
		return null;
	}

	private boolean areSame(Episode ep1, Episode ep2) {
		return sameSeriesAndEpisodeNumber(ep1, ep2) || sameTitle(ep1, ep2);
	}

	private boolean sameTitle(Item ep1, Item ep2) {
		return Strings.nullToEmpty(ep1.getTitle()).equals(ep2.getTitle());
	}

	private boolean sameSeriesAndEpisodeNumber(Episode ep1, Episode ep2) {
		if (ep1.getEpisodeNumber() == null || ep1.getSeriesNumber() == null) {
			return false;
		}
		return ep1.getSeriesNumber().equals(ep2.getSeriesNumber()) && ep1.getEpisodeNumber().equals(ep2.getEpisodeNumber());
	}
}
