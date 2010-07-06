package org.uriplay.persistence.equiv;

import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.uriplay.media.entity.Content;
import org.uriplay.persistence.content.ContentResolver;

import com.google.common.collect.Sets;

public class EquivalentContentFinder {

	private final Log log = LogFactory.getLog(getClass());
	
	private final EquivalentUrlFinder urlFinder;
	private final ContentResolver resolver;

	public EquivalentContentFinder(EquivalentUrlFinder urlFinder, ContentResolver resolver) {
		this.urlFinder = urlFinder;
		this.resolver = resolver;
	}
	
	public Iterable<? extends Content> equivalentTo(Content content) {
		Set<String> equivalentUrls = urlFinder.get(content.getAllUris());

		Set<Content> equivalentContent = Sets.newHashSet();
		
		for(String uri: equivalentUrls) {
			try {
				Content found = resolver.findByUri(uri);
				if (found != null) {
					equivalentContent.add(found);
				}
			} catch (Exception e) {
				log.warn(e);
			}
		}
		return equivalentContent;
	}

}
