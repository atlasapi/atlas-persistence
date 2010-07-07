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
	
	public EquivalentContent equivalentTo(Content content) {
		Set<String> equivalentUrls = urlFinder.get(content.getAllUris());
		equivalentUrls.remove(content.getCanonicalUri());
		
		
		Set<Content> equivalentContent = Sets.newHashSet();
		Set<String> lookupsThatErrored = Sets.newHashSet();
		
		for(String uri: equivalentUrls) {
			try {
				Content found = resolver.findByUri(uri);
				if (found != null && !found.equals(content)) {
					equivalentContent.add(found);
				}
			} catch (Exception e) {
				lookupsThatErrored.add(uri);
				log.warn(e);
			}
		}
		
		
		// Don't mark uris and 'not resolved' if there was an error when fetching it because it probably means that there is content
		// at that location, but that the service is down or has changed its data format
		Set<String> notResolved = Sets.difference(equivalentUrls, Sets.union(lookupsThatErrored, canonicalUrisFrom(equivalentContent)));
		return new EquivalentContent(equivalentContent, notResolved);
	}
	
	private Set<String> canonicalUrisFrom(Set<Content> equivalentContent) {
		Set<String> uris = Sets.newHashSet();
		for (Content content : equivalentContent) {
			uris.add(content.getCanonicalUri());
		}
		return uris;
	}

	class EquivalentContent {

		private final Set<Content> equivalentContent;
		private final Set<String> notResolved;

		public EquivalentContent(Set<Content> equivalentContent, Set<String> notResolved) {
			this.equivalentContent = equivalentContent;
			this.notResolved = notResolved;
		}
		
		public Set<Content> equivalent() {
			return equivalentContent;
		}
		
		public Set<String> probableAliases() {
			return notResolved;
		}
	}

}
