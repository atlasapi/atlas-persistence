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

package org.atlasapi.persistence.content.mongo;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.atlasapi.content.criteria.ContentQuery;
import org.atlasapi.content.criteria.MatchesNothing;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Schedule;
import org.atlasapi.persistence.content.query.KnownTypeQueryExecutor;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import com.metabroadcast.common.query.Selection;

@SuppressWarnings("unchecked")
public class MongoDBQueryExecutor implements KnownTypeQueryExecutor {
	
	private final QueryResultTrimmer trimmer = new QueryResultTrimmer();
	private final MongoDbBackedContentStore roughSearch;
	
	private boolean filterUriQueries = false;
	
	public MongoDBQueryExecutor(MongoDbBackedContentStore roughSearch) {
		this.roughSearch = roughSearch;
	}
	
	private <T extends Identified> List<T> sort(List<T> content, final Iterable<String> orderIterable) {
		
		final ImmutableList<String> order = ImmutableList.copyOf(orderIterable);
		
		Comparator<Identified> byPositionInList = new Comparator<Identified>() {

			@Override
			public int compare(Identified c1, Identified c2) {
				return Ints.compare(indexOf(c1), indexOf(c2));
			}

			private int indexOf(Identified content) {
				for (String uri : content.getAllUris()) {
					int idx = order.indexOf(uri);
					if (idx != -1) {
						return idx;
					}
				}
				if (content.getCurie() != null) {
					return order.indexOf(content.getCurie());
				}
				return -1;
			}
		};
		
		List<T> toSort = Lists.newArrayList(content);
		Collections.sort(toSort, byPositionInList);
		return toSort;
	}
	
	@Override
	public List<Content> discover(ContentQuery query) {
		if (MatchesNothing.isEquivalentTo(query)) {
			return Collections.emptyList();
		}
		return executeDiscoverQuery(query);
	}

	private List<Content> executeDiscoverQuery(ContentQuery query) {
		List<? extends Content> contents = roughSearch.discover(query);
		
		if (contents.isEmpty()) {
			return ImmutableList.of();
		}

		return (List<Content>) filter(query, contents, true);
	}

	@Override
	public List<Identified> executeUriQuery(Iterable<String> uris, ContentQuery query) {
		if (MatchesNothing.isEquivalentTo(query)) {
			return Collections.emptyList();
		}
		List<? extends Identified> content = roughSearch.findByUriOrAlias(uris);

		if (content.isEmpty()) {
			return Collections.emptyList();
		}
		
		return sort((List<Identified>) filter(query, content, filterUriQueries), uris);
	}

	private <T  extends Identified> List<T> filter(ContentQuery query, List<T> brands, boolean removeItemsThatDontMatch) {
		return trimmer.trim(brands, query, removeItemsThatDontMatch);
	}

	public void setFilterUriQueries(boolean filterUriQueries) {
		this.filterUriQueries = filterUriQueries;
	}

	@Override
	public Schedule schedule(ContentQuery query) {
		if (!Selection.ALL.equals(query.getSelection())) {
			throw new IllegalArgumentException("Cannot paginate schedule queries using limit and offset, change the transmission window instead");
		}
		List<Content> found = discover(query);
		
		return Schedule.fromItems(Iterables.concat(Iterables.transform(found, TO_ITEMS)));
	}
	
	private static final Function<Content, List<Item>> TO_ITEMS = new Function<Content, List<Item>>() {

		@Override
		public List<Item> apply(Content input) {
			if (input instanceof Item) {
				return ImmutableList.of((Item) input);
			} else {
				return ((Container<Item>) input).getContents();
			}
		}
	};
}
