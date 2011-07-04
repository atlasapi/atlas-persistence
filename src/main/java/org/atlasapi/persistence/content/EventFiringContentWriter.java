package org.atlasapi.persistence.content;

import java.util.Collections;

import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Item;
import org.atlasapi.persistence.content.ContentListener.ChangeType;

import com.google.common.collect.ImmutableList;

public class EventFiringContentWriter implements ContentWriter {

	private final ContentWriter delegate;
	private final ContentListener listener;
	
	public EventFiringContentWriter(ContentWriter delegate, ContentListener listener) {
		this.delegate = delegate;
		this.listener = listener;
	}
	
	@Override
	public void createOrUpdate(Container container) {
		delegate.createOrUpdate(container);
		notifyListener(container);
	}
	
	@Override
	public void createOrUpdate(Item item) {
		delegate.createOrUpdate(item);
		notifyListener(item);
	}
    
    private void notifyListener(Container container) {
    	listener.brandChanged(ImmutableList.<Container>of(container), ChangeType.CONTENT_UPDATE);
    }
    
    private void notifyListener(Item item) {
        listener.itemChanged(Collections.singletonList(item), ChangeType.CONTENT_UPDATE);
    }
}
