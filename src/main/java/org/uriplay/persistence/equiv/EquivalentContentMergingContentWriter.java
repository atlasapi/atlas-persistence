package org.uriplay.persistence.equiv;

import java.util.Set;

import org.uriplay.media.entity.Content;
import org.uriplay.media.entity.Item;
import org.uriplay.media.entity.Playlist;
import org.uriplay.persistence.content.ContentWriter;

public class EquivalentContentMergingContentWriter implements ContentWriter {

	private final ContentWriter delegate;

	public EquivalentContentMergingContentWriter(ContentWriter delegate) {
		this.delegate = delegate;
	}

	@Override
	public void addAliases(String uri, Set<String> aliases) {
		delegate.addAliases(uri, aliases);
	}

	@Override
	public void createOrUpdateContent(Content bean, boolean markMissingItemsAsUnavailable) {
		delegate.createOrUpdateContent(bean, markMissingItemsAsUnavailable);
	}

	@Override
	public void createOrUpdateItem(Item item) {
		delegate.createOrUpdateItem(item);
	}

	@Override
	public void createOrUpdatePlaylist(Playlist enclosingList, boolean markMissingItemsAsUnavailable) {
		delegate.createOrUpdatePlaylist(enclosingList, markMissingItemsAsUnavailable);
	}
}
