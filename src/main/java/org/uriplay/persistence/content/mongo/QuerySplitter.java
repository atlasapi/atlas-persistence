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

import java.util.List;
import java.util.Set;

import org.uriplay.content.criteria.AtomicQuery;
import org.uriplay.content.criteria.AttributeQuery;
import org.uriplay.content.criteria.BooleanAttributeQuery;
import org.uriplay.content.criteria.ContentQuery;
import org.uriplay.content.criteria.DateTimeAttributeQuery;
import org.uriplay.content.criteria.EnumAttributeQuery;
import org.uriplay.content.criteria.IntegerAttributeQuery;
import org.uriplay.content.criteria.MatchesNothing;
import org.uriplay.content.criteria.QueryVisitor;
import org.uriplay.content.criteria.StringAttributeQuery;
import org.uriplay.media.entity.Description;

import com.google.common.collect.Iterables;
import com.metabroadcast.common.base.Maybe;

public class QuerySplitter {

	public Maybe<ContentQuery> retain(ContentQuery query,  Set<Class<? extends Description>> allowedTypes) {
		return split(query, allowedTypes, true);
	}
	
	public Maybe<ContentQuery> discard(ContentQuery query, Set<Class<? extends Description>> allowedTypes) {
		return split(query, allowedTypes, false);
	}
	
	private Maybe<ContentQuery> split(ContentQuery query, final Set<Class<? extends Description>> allowedTypes, final boolean retain) {
		
		List<Maybe<AtomicQuery>> extracted = query.accept(new QueryVisitor<Maybe<AtomicQuery>>() {

			@Override
			public Maybe<AtomicQuery> visit(IntegerAttributeQuery query) {
				return allowed(query);
			}

			@Override
			public Maybe<AtomicQuery> visit(StringAttributeQuery query) {
				return allowed(query);
			}

			@Override
			public Maybe<AtomicQuery> visit(BooleanAttributeQuery query) {
				return allowed(query);
			}

			@Override
			public Maybe<AtomicQuery> visit(EnumAttributeQuery<?> query) {
				return allowed(query);
			}

			@Override
			public Maybe<AtomicQuery> visit(DateTimeAttributeQuery query) {
				return allowed(query);
			}

			@Override
			public Maybe<AtomicQuery> visit(MatchesNothing noOp) {
				return Maybe.nothing();
			}
			
			private Maybe<AtomicQuery> allowed(AttributeQuery<?> query) {
				if (allowedTypes.contains(query.getAttribute().target())) {
					return retain ? Maybe.<AtomicQuery>just(query) : Maybe.<AtomicQuery>nothing();
				} else {
					return retain ? Maybe.<AtomicQuery>nothing() : Maybe.<AtomicQuery>just(query);
				}
			}
		});
		
		Iterable<AtomicQuery> operands = Maybe.filterValues(extracted);
		if (Iterables.isEmpty(operands)) {
			return Maybe.nothing();
		} else {
			return Maybe.just(query.copyWithOperands(operands));
		}
		
	}
}
