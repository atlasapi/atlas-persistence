package org.atlasapi.persistence.equiv;

import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.atlasapi.media.entity.Content;
import org.atlasapi.persistence.content.ContentResolver;

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
		
		Set<Content> equivalentContent = Sets.newHashSet();
		Set<String> allEquivalentUris = Sets.newHashSet();
		
		for(String uri: urlFinder.get(content.getAllUris())) {
			allEquivalentUris.add(uri);
			try {
				Content found = resolver.findByUri(uri);
				if (found != null) {
					allEquivalentUris.addAll(found.getAllUris());
					if (!found.getCanonicalUri().equals(content.getCanonicalUri())) {
						equivalentContent.add(found);
					}
				}
			} catch (Exception e) {
				log.warn(e);
			}
		}
		
		
		allEquivalentUris.remove(content.getCanonicalUri());
		
		return new EquivalentContent(equivalentContent, allEquivalentUris);
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
