/* Copyright 2010 Meta Broadcast Ltd

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License. You may
obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License. */

package org.uriplay.persistence.content.mongo;

import static org.uriplay.persistence.content.mongo.QueryConcernsTypeDecider.concernsType;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.uriplay.content.criteria.ContentQuery;
import org.uriplay.media.entity.Encoding;
import org.uriplay.media.entity.Item;
import org.uriplay.media.entity.Location;
import org.uriplay.media.entity.Version;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.metabroadcast.common.base.Maybe;

/**
 * This class filters associations ({@link Version}s, {@link Encoding}s etc) that don't match the query.
 * Since searches are performed on denormalised representations of {@link Item}s, search results may include false
 * positives, which are filtered by this class.
 * 
 * @author John Ayres (john@metabroadcast.com)
 */
public class QueryResultTrimmer {
	
	public List<Item> trim(List<Item> items, ContentQuery query, boolean removeItemsThatDontMatch) {
		List<Item> trimmed = Lists.newArrayListWithCapacity(items.size());
		for (Item item : items) {
			if (trim(item, query) || !removeItemsThatDontMatch) {
				trimmed.add(item);
			}
		}
		return trimmed;
	}
	
	/**
	 * Trims associations that don't match the query
	 * @return true if the item matches, false if the item should not be included in the result set 
	 */
	private boolean trim(Item item, ContentQuery query) {
		Maybe<Set<Version>> versions = trimVersions(item.getVersions(), query);
		item.setVersions(versions.valueOrDefault(Sets.<Version>newHashSet()));
		if (versions.isNothing()) {
			return false;
		} else {
			return true;
		}
	}

	@SuppressWarnings("unchecked")
	private Maybe<Set<Version>> trimVersions(Set<Version> versions, ContentQuery query) {
		if (versions.isEmpty() && !concernsType(query, Version.class, Encoding.class, Location.class)) {
			return Maybe.just(Collections.<Version>emptySet());
		}
		Set<Version> trimmedVersions = Sets.newHashSetWithExpectedSize(versions.size());
		for (Version version : versions) {
			if (check(query,version)) {
				Maybe<Set<Encoding>> trimmedEncodings = trimEncodings(version.getManifestedAs(), query);
				if (trimmedEncodings.hasValue()) {
					version.setManifestedAs(trimmedEncodings.requireValue());
					trimmedVersions.add(version);
				}
			}
		}
		return nothingIfEmpty(trimmedVersions);
	}

	@SuppressWarnings("unchecked")
	private Maybe<Set<Encoding>> trimEncodings(Set<Encoding> encodings, ContentQuery query) {
		if (encodings.isEmpty() && !concernsType(query, Encoding.class, Location.class)) {
			return Maybe.just(Collections.<Encoding>emptySet());
		}
		Set<Encoding> trimmedEncodings = Sets.newHashSetWithExpectedSize(encodings.size());
		for (Encoding encoding : encodings) {
			if (check(query, encoding)) {
				Maybe<Set<Location>> trimmedLocations = trimLocations(encoding.getAvailableAt(), query);
				if (trimmedLocations.hasValue()) {
					encoding.setAvailableAt(trimmedLocations.requireValue());
					trimmedEncodings.add(encoding);
				}
			}
		}
		return nothingIfEmpty(trimmedEncodings);
	}

	@SuppressWarnings("unchecked")
	private Maybe<Set<Location>> trimLocations(Set<Location> locations, ContentQuery query) {
		if (locations.isEmpty() && concernsType(query, Location.class)) {
			return Maybe.just(Collections.<Location>emptySet());
		}
		Set<Location> trimmedLocations = Sets.newHashSetWithExpectedSize(locations.size());
		for (Location location : locations) {
			if (check(query, location)) {
				trimmedLocations.add(location);
			}
		}
		return nothingIfEmpty(trimmedLocations);
	}

	private boolean check(ContentQuery query, Version version) {
		return new InMemoryQueryResultChecker(version).check(query);
	}
	
	private boolean check(ContentQuery query, Encoding encoding) {
		return new InMemoryQueryResultChecker(encoding).check(query);
	}
	
	private boolean check(ContentQuery query, Location location) {
		return new InMemoryQueryResultChecker(location).check(query);
	}

	private static <T> Maybe<Set<T>> nothingIfEmpty(Set<T> elems) {
		return elems.isEmpty() ? Maybe.<Set<T>>nothing() : Maybe.just(elems);
	}
}
