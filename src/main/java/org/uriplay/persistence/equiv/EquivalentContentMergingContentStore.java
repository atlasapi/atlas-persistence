package org.uriplay.persistence.equiv;

import java.util.Set;

import org.uriplay.media.entity.Content;
import org.uriplay.media.entity.Item;
import org.uriplay.media.entity.Playlist;
import org.uriplay.persistence.content.ContentWriter;

public class EquivalentContentMergingContentStore implements ContentWriter {

	@Override
	public void addAliases(String uri, Set<String> aliases) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void createOrUpdateContent(Content bean, boolean markMissingItemsAsUnavailable) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void createOrUpdateItem(Item item) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void createOrUpdatePlaylist(Playlist enclosingList, boolean markMissingItemsAsUnavailable) {
		// TODO Auto-generated method stub
	}

}
