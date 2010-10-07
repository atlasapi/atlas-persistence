package org.atlasapi.persistence.equiv;

import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Playlist;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.DefinitiveContentWriter;

public class EquivalentContentMergingContentWriter implements ContentWriter, DefinitiveContentWriter {

	private final ContentWriter delegate;
	private final EquivalentContentMerger merger;
    private final DefinitiveContentWriter definitiveWriter;

	public EquivalentContentMergingContentWriter(ContentWriter delegate, DefinitiveContentWriter definitiveWriter, EquivalentContentMerger merger) {
		this.delegate = delegate;
        this.definitiveWriter = definitiveWriter;
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

    @Override
    public void createOrUpdateDefinitiveItem(Item item) {
        definitiveWriter.createOrUpdateDefinitiveItem(merger.merge(item));
    }

    @Override
    public void createOrUpdateDefinitivePlaylist(Playlist playlist) {
        definitiveWriter.createOrUpdateDefinitivePlaylist(merger.merge(playlist));
    }

	@Override
	public void createOrUpdatePlaylistSkeleton(Playlist playlist) {
		delegate.createOrUpdatePlaylistSkeleton(playlist);
	}
}
