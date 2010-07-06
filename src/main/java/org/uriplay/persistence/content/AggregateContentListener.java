package org.uriplay.persistence.content;

import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.uriplay.media.entity.Brand;
import org.uriplay.media.entity.Item;

import com.google.common.collect.Sets;

public class AggregateContentListener implements ContentListener, ApplicationContextAware {

	private static final Log log = LogFactory.getLog(AggregateContentListener.class);

	private final Collection<ContentListener> listeners = Sets.newHashSet();

	
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

	@Override
	public void setApplicationContext(ApplicationContext context) throws BeansException {
		Collection<ContentListener> inContext = context.getBeansOfType(ContentListener.class).values();
		inContext.remove(this);
		listeners.addAll(inContext);
	}
}
