package org.atlasapi.persistence.content;

import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Item;

import com.google.common.collect.Sets;

public class AggregateContentListener implements ContentListener {

	private static final Log log = LogFactory.getLog(AggregateContentListener.class);

	private final Collection<ContentListener> listeners = Sets.newHashSet();
	
	@Override
	public void brandChanged(Iterable<? extends Container<?>> container, ChangeType changeType) {
		for (ContentListener listener : listeners) {
			try {
				listener.brandChanged(container, changeType);
			} catch (Exception e) {
				log.info("Got exception informing listener of brand change exception was: " + e);
			}
		}
	}
	
	public void itemChanged(Iterable<? extends Item> items, ChangeType changeType) {
		for (ContentListener listener : listeners) {
			try {
				listener.itemChanged(items, changeType);
			} catch (Exception e) {
				log.info("Got exception informing listener of item change exception was: " + e);
			}
		}
	}
	
	public void addListener(ContentListener listener) {
	    listeners.add(listener);
	}

}
