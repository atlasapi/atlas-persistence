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

import java.util.Set;

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

import com.google.common.collect.Sets;

public class QueryConcernsTypeDecider {

	public static boolean concernsType(final ContentQuery query, final Class<? extends Description>... type) {
		final Set<Class<? extends Description>> typeLookup = Sets.newHashSet(type);
		
		return query.accept(new QueryVisitor<Boolean>() {

			@Override
			public Boolean visit(IntegerAttributeQuery query) {
				return typeLookup.contains(query.getAttribute().target());
			}

			@Override
			public Boolean visit(StringAttributeQuery query) {
				return typeLookup.contains(query.getAttribute().target());
			}

			@Override
			public Boolean visit(BooleanAttributeQuery query) {
				return typeLookup.contains(query.getAttribute().target());
			}

			@Override
			public Boolean visit(EnumAttributeQuery<?> query) {
				return typeLookup.contains(query.getAttribute().target());
			}

			@Override
			public Boolean visit(DateTimeAttributeQuery query) {
				return typeLookup.contains(query.getAttribute().target());
			}

			@Override
			public Boolean visit(MatchesNothing noOp) {
				return false;
			}

			@Override
			public Boolean visit(ConjunctiveQuery conjunctiveQuery) {
				return visitJunction(conjunctiveQuery);
			}
			
			private Boolean visitJunction(LogicalOperatorQuery query) {
				for (ContentQuery subQuery : query.operands()) {
					if (concernsType(subQuery, type)) {
						return true;
					}
				}
				return false;
			}
		});
	}
}
