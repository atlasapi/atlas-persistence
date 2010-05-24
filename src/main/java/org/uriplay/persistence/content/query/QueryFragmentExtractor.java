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

package org.uriplay.persistence.content.query;

import java.util.List;
import java.util.Set;

import org.uriplay.content.criteria.AttributeQuery;
import org.uriplay.content.criteria.ConjunctiveQuery;
import org.uriplay.content.criteria.ContentQuery;
import org.uriplay.content.criteria.QueryVisitorAdapter;
import org.uriplay.content.criteria.attribute.Attribute;
import org.uriplay.content.criteria.attribute.StringValuedAttribute;

import com.google.common.collect.Lists;
import com.metabroadcast.common.base.Maybe;


public class QueryFragmentExtractor {

	/**
	 * Extracts the part of the query that concern {@link StringValuedAttribute}s passed
	 * Used to extract URI and CURIE query parts.
	 */
	public static Maybe<ContentQuery> extract(ContentQuery query, final Set<Attribute<?>> attributes) {
		return query.accept(new QueryVisitorAdapter<Maybe<ContentQuery>>() {
			
			
			@Override
			public  Maybe<ContentQuery> visit(ConjunctiveQuery conjunctiveQuery) {
				List<ContentQuery> matchingQueries = Lists.newArrayList();
				
				for (ContentQuery operand : conjunctiveQuery.operands()) {
					Maybe<ContentQuery> subMatch = operand.accept(this);
					if (subMatch.hasValue()) {
						matchingQueries.add(subMatch.requireValue());
					}
				}
				if (matchingQueries.isEmpty()) {
					return Maybe.nothing();
				}
				return Maybe.<ContentQuery>just(conjunctiveQuery.copyWithOperands(matchingQueries));
			}
			
			@Override
			protected Maybe<ContentQuery> defaultValue(ContentQuery query) {
				if (query instanceof AttributeQuery<?>) {
					if (attributes.contains(((AttributeQuery<?>) query).getAttribute())) {
						return Maybe.<ContentQuery>just(query);
					} 
				}
				return Maybe.nothing();
			}
		});
	}
}
