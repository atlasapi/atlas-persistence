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

package org.uriplay.persistence.content.mongodb;

import java.util.List;
import java.util.Set;

import org.jherd.util.Maybe;
import org.uriplay.content.criteria.AttributeQuery;
import org.uriplay.content.criteria.BooleanAttributeQuery;
import org.uriplay.content.criteria.ConjunctiveQuery;
import org.uriplay.content.criteria.ContentQuery;
import org.uriplay.content.criteria.DateTimeAttributeQuery;
import org.uriplay.content.criteria.EnumAttributeQuery;
import org.uriplay.content.criteria.IntegerAttributeQuery;
import org.uriplay.content.criteria.LogicalOperatorQuery;
import org.uriplay.content.criteria.MatchesNothing;
import org.uriplay.content.criteria.QueryVisitor;
import org.uriplay.content.criteria.StringAttributeQuery;
import org.uriplay.media.entity.Description;

import com.google.common.collect.Iterables;
import com.google.soy.common.collect.Lists;

public class QuerySplitter {

	public Maybe<ContentQuery> retain(ContentQuery query,  Set<Class<? extends Description>> allowedTypes) {
		return split(query, allowedTypes, true);
	}
	
	public Maybe<ContentQuery> discard(ContentQuery query, Set<Class<? extends Description>> allowedTypes) {
		return split(query, allowedTypes, false);
	}
	
	private Maybe<ContentQuery> split(ContentQuery query, final Set<Class<? extends Description>> allowedTypes, final boolean retain) {
		return query.accept(new QueryVisitor<Maybe<ContentQuery>>() {

			@Override
			public Maybe<ContentQuery> visit(IntegerAttributeQuery query) {
				return allowed(query);
			}

			@Override
			public Maybe<ContentQuery> visit(StringAttributeQuery query) {
				return allowed(query);
			}

			@Override
			public Maybe<ContentQuery> visit(BooleanAttributeQuery query) {
				return allowed(query);
			}

			@Override
			public Maybe<ContentQuery> visit(EnumAttributeQuery<?> query) {
				return allowed(query);
			}

			@Override
			public Maybe<ContentQuery> visit(DateTimeAttributeQuery query) {
				return allowed(query);
			}

			@Override
			public Maybe<ContentQuery> visit(ConjunctiveQuery query) {
				return junction(query);
			}

			private Maybe<ContentQuery> junction(LogicalOperatorQuery query) {
				List<ContentQuery> splits = Lists.newArrayList();
				for (ContentQuery subQuery : query.operands()) {
					Maybe<ContentQuery> split = split(subQuery, allowedTypes, retain);
					if (split.hasValue()) {
						splits.add(split.requireValue());
					}
				}
				if (splits.isEmpty()) {
					return Maybe.nothing();
				}
				if (splits.size() == 1) {
					ContentQuery element = Iterables.getOnlyElement(splits);
					if (query.getSelection() != null) {
						element.withSelection(query.getSelection());
						return Maybe.just(element);
					}
				}
				return Maybe.<ContentQuery>just(query.copyWithOperands(splits));
			}

			@Override
			public Maybe<ContentQuery> visit(MatchesNothing noOp) {
				return Maybe.nothing();
			}
			
			private Maybe<ContentQuery> allowed(AttributeQuery<?> query) {
				if (allowedTypes.contains(query.getAttribute().target())) {
					return retain ? Maybe.<ContentQuery>just(query) : Maybe.<ContentQuery>nothing();
				} else {
					return retain ? Maybe.<ContentQuery>nothing() : Maybe.<ContentQuery>just(query);
				}
			}
		});
	}
}
