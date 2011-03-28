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
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.persistence.content.query.KnownTypeQueryExecutor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;

@SuppressWarnings("unchecked")
public class MongoDBQueryExecutor implements KnownTypeQueryExecutor {
	
	private final QueryResultTrimmer trimmer = new QueryResultTrimmer();
	private final MongoDbBackedContentStore roughSearch;
	
	public MongoDBQueryExecutor(MongoDbBackedContentStore roughSearch) {
		this.roughSearch = roughSearch;
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
		
		return Identified.sort((List<Identified>) filter(query, content, false), uris);
	}

	private <T  extends Identified> List<T> filter(ContentQuery query, List<T> brands, boolean removeItemsThatDontMatch) {
		return trimmer.trim(brands, query, removeItemsThatDontMatch);
	}
}
