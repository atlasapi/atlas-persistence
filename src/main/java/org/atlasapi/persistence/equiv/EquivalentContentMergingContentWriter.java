package org.atlasapi.persistence.equiv;

import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.ContentGroup;
import org.atlasapi.media.entity.Item;
import org.atlasapi.persistence.content.ContentWriter;

public class EquivalentContentMergingContentWriter implements ContentWriter {

	private final ContentWriter delegate;
	private final EquivalentContentMerger merger;

	public EquivalentContentMergingContentWriter(ContentWriter delegate, EquivalentContentMerger merger) {
		this.delegate = delegate;
		this.merger = merger;
	}

	@Override
	public void createOrUpdate(Item item) {
		delegate.createOrUpdate(merger.merge(item));
	}

	@Override
	public void createOrUpdate(Container<?> enclosingList, boolean markMissingItemsAsUnavailable) {
		delegate.createOrUpdate(merger.merge(enclosingList), markMissingItemsAsUnavailable);
	}
   
	@Override
	public void createOrUpdateSkeleton(ContentGroup playlist) {
		delegate.createOrUpdateSkeleton(playlist);
	}
}
