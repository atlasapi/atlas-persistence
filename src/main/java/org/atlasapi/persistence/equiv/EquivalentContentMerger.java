package org.atlasapi.persistence.equiv;

import java.util.Set;

import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Version;
import org.atlasapi.persistence.equiv.EquivalentContentFinder.EquivalentContent;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

public class EquivalentContentMerger {

	private final EquivalentContentFinder finder;

	public EquivalentContentMerger(EquivalentContentFinder finder) {
		this.finder = finder;
	}
	
	Item merge(Item item) {
		// for now don't merge items directly
		return item;
	}

	Container<?> merge(Container<?> container) {
		EquivalentContent equiv = finder.equivalentTo(container);
		Iterable<? extends Content> equivContent = equiv.equivalent();
		for (Content content : equivContent) {
			if (content instanceof Container<?>) {
				mergeSubItemsOf(container, (Container<?>) content);
			}
			container.addEquivalentTo(content);
		}
		for (String alias : equiv.probableAliases()) {
			container.addAlias(alias);
		}
		return container;
	}

	private void mergeSubItemsOf(Container<?> original, Container<?> equivTo) {
		for (Item item : original.getContents()) {
			Item sameItem = findSameItem(item, equivTo.getContents());
			if (sameItem != null) {
				// remove previously merged versions
				Set<Version> versions = Sets.newHashSet(Iterables.filter(item.getVersions(), Predicates.not(isProvidedBy(sameItem.getPublisher()))));
				versions.addAll(sameItem.nativeVersions());
				item.setVersions(versions);
				item.addEquivalentTo(sameItem);
			}
		}
	}
	
	private static Predicate<Version> isProvidedBy(final Publisher publisher) {
		return new Predicate<Version>() {
	
			@Override
			public boolean apply(Version input) {
				return input.getProvider() != null && input.getProvider().equals(publisher);
			}
			
		};
	}

	private Item findSameItem(Item needle, Iterable<? extends Item> haystack) {
	    if (needle instanceof Episode) {
    		for (Item item : haystack) {
    			if (item instanceof Episode && areSame((Episode) needle, (Episode) item)) {
    				return item;
    			}
    		}
	    }
		return null;
	}

	private boolean areSame(Episode ep1, Episode ep2) {
		return sameSeriesAndEpisodeNumber(ep1, ep2);
	}

	private boolean sameTitle(Item ep1, Item ep2) {
		return Strings.nullToEmpty(ep1.getTitle()).equals(ep2.getTitle());
	}

	private boolean sameSeriesAndEpisodeNumber(Episode ep1, Episode ep2) {
		if (ep1.getEpisodeNumber() == null || ep1.getSeriesNumber() == null) {
			return false;
		}
		return ep1.getSeriesNumber().equals(ep2.getSeriesNumber()) && ep1.getEpisodeNumber().equals(ep2.getEpisodeNumber());
	}
}
