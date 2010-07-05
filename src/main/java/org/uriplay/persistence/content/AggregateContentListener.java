package org.uriplay.persistence.content;

import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.uriplay.media.entity.Brand;
import org.uriplay.media.entity.Item;

public class AggregateContentListener implements ContentListener {

	private static final Log log = LogFactory.getLog(AggregateContentListener.class);

	private final Collection<? extends ContentListener> listeners;

	public AggregateContentListener(Collection<? extends ContentListener> listeners) {
		this.listeners = listeners;
	}
	
	public void brandChanged(Collection<Brand> brands, changeType changeType) {
		for (ContentListener listener : listeners) {
			try {
				listener.brandChanged(brands, changeType);
			} catch (Exception e) {
				log.info("Got exception informing listener of brand change exception was: " + e);
			}
		}
	}
	
	public void itemChanged(Collection<Item> items, changeType changeType) {
		for (ContentListener listener : listeners) {
			try {
				listener.itemChanged(items, changeType);
			} catch (Exception e) {
				log.info("Got exception informing listener of item change exception was: " + e);
			}
		}
	}
	
}
