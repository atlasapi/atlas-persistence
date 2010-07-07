package org.atlasapi.persistence.equiv;

import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Playlist;
import org.atlasapi.persistence.content.ContentWriter;

public class EquivalentContentMergingContentWriter implements ContentWriter {

	private final ContentWriter delegate;
	private final EquivalentContentMerger merger;

	public EquivalentContentMergingContentWriter(ContentWriter delegate, EquivalentContentMerger merger) {
		this.delegate = delegate;
		this.merger = merger;
	}

	@Override
	public void createOrUpdateItem(Item item) {
		delegate.createOrUpdateItem(merger.merge(item));
	}

	@Override
	public void createOrUpdatePlaylist(Playlist enclosingList, boolean markMissingItemsAsUnavailable) {
		delegate.createOrUpdatePlaylist(merger.merge(enclosingList), markMissingItemsAsUnavailable);
	}
}
