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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

import org.joda.time.DateTime;
import org.uriplay.content.criteria.BooleanAttributeQuery;
import org.uriplay.content.criteria.ConjunctiveQuery;
import org.uriplay.content.criteria.ContentQuery;
import org.uriplay.content.criteria.DateTimeAttributeQuery;
import org.uriplay.content.criteria.EnumAttributeQuery;
import org.uriplay.content.criteria.IntegerAttributeQuery;
import org.uriplay.content.criteria.MatchesNothing;
import org.uriplay.content.criteria.QueryVisitor;
import org.uriplay.content.criteria.StringAttributeQuery;
import org.uriplay.content.criteria.attribute.Attribute;
import org.uriplay.content.criteria.attribute.Attributes;
import org.uriplay.content.criteria.operator.BooleanOperatorVisitor;
import org.uriplay.content.criteria.operator.DateTimeOperatorVisitor;
import org.uriplay.content.criteria.operator.EnumOperatorVisitor;
import org.uriplay.content.criteria.operator.IntegerOperatorVisitor;
import org.uriplay.content.criteria.operator.StringOperatorVisitor;
import org.uriplay.content.criteria.operator.Operators.After;
import org.uriplay.content.criteria.operator.Operators.Before;
import org.uriplay.content.criteria.operator.Operators.Beginning;
import org.uriplay.content.criteria.operator.Operators.Equals;
import org.uriplay.content.criteria.operator.Operators.GreaterThan;
import org.uriplay.content.criteria.operator.Operators.LessThan;
import org.uriplay.content.criteria.operator.Operators.Search;
import org.uriplay.media.entity.Brand;
import org.uriplay.media.entity.Broadcast;
import org.uriplay.media.entity.Description;
import org.uriplay.media.entity.Encoding;
import org.uriplay.media.entity.Item;
import org.uriplay.media.entity.Location;
import org.uriplay.media.entity.Playlist;
import org.uriplay.media.entity.Version;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class MongoDBQueryBuilder {

	private static final String MONGO_GREATER_THAN = "$gt";
	private static final String MONGO_LESS_THAN = "$lt";
	private static final String MONGO_IN = "$in";

	public DBObject buildPlaylistQuery(ContentQuery query) {
		return buildQuery(query, Playlist.class);
	}
	
	public DBObject buildBrandQuery(ContentQuery query) {
		return buildQuery(query, Brand.class);
	}
	
	public DBObject buildItemQuery(ContentQuery query) {
		return buildQuery(query, Item.class);
	}

	private DBObject buildQuery(ContentQuery query, final Class<? extends Description> queryType) {
		
		return query.accept(new QueryVisitor<DBObject>() {

			@Override
			@SuppressWarnings("unchecked")
			public DBObject visit(IntegerAttributeQuery query) {
				final List<Integer> values = (List<Integer>) query.getValue();
				
				BasicDBObject rhs = query.accept(new IntegerOperatorVisitor<BasicDBObject>() {

					@Override
					public BasicDBObject visit(Equals equals) {
						return new BasicDBObject(MONGO_IN, list(values));
					}

					@Override
					public BasicDBObject visit(LessThan lessThan) {
						return new BasicDBObject(MONGO_LESS_THAN, Collections.max(values));
					}

					@Override
					public BasicDBObject visit(GreaterThan greaterThan) {
						return new BasicDBObject(MONGO_GREATER_THAN, Collections.min(values));
					}
				});
				
				return new BasicDBObject(fullyQualifiedPath(queryType, query.getAttribute()), rhs);
			}

			@Override
			public BasicDBObject visit(final StringAttributeQuery query) {
				final BasicDBList values = new BasicDBList();
				values.addAll(query.getValue());
				
				return query.accept(new StringOperatorVisitor<BasicDBObject>() {

					@Override
					public BasicDBObject visit(Equals equals) {
						return new BasicDBObject(fullyQualifiedPath(queryType, query.getAttribute()), new BasicDBObject(MONGO_IN, values));
					}

					@Override
					public BasicDBObject visit(Beginning beginning) {
						Pattern pattern = Pattern.compile("^" + (String) query.getValue().get(0), Pattern.CASE_INSENSITIVE);
						return new BasicDBObject(fullyQualifiedPath(queryType, query.getAttribute()), pattern);
					}

					@Override
					public BasicDBObject visit(Search search) {
						throw new UnsupportedOperationException();
					}
				});
				
			}

			@Override
			public BasicDBObject visit(final BooleanAttributeQuery query) {
				
				final Boolean value = (Boolean) query.getValue().get(0);
				
				return query.accept(new BooleanOperatorVisitor<BasicDBObject>() {

					@Override
					public BasicDBObject visit(Equals equals) {
						return new BasicDBObject(fullyQualifiedPath(queryType, query.getAttribute()), value);
					}
				});
				
			}

			@Override
			@SuppressWarnings("unchecked")
			public BasicDBObject visit(final EnumAttributeQuery<?> query) {
				
				final List<Enum<?>> values = (List<Enum<?>>) query.getValue();
				
				return query.accept(new EnumOperatorVisitor<BasicDBObject>() {

					@Override
					public BasicDBObject visit(Equals equals) {
						return new BasicDBObject(fullyQualifiedPath(queryType, query.getAttribute()), new BasicDBObject(MONGO_IN, list(toLowercaseStrings(values))));
					}

					private Collection<?> toLowercaseStrings(Collection<Enum<?>> values) {
						List<String> strings = Lists.newArrayList();
						for (Enum<?> value : values) {
							strings.add(value.toString().toLowerCase());
						}
						return strings;
					}
				});
			}

			@SuppressWarnings("unchecked")
            @Override
			public BasicDBObject visit(DateTimeAttributeQuery query) {
				final List<Long> values = toMillis((List<DateTime>) query.getValue());

				BasicDBObject rhs = query.accept(new DateTimeOperatorVisitor<BasicDBObject>() {

					@Override
					public BasicDBObject visit(Before before) {
						return new BasicDBObject(MONGO_LESS_THAN, Collections.max(values));
					}

					@Override
					public BasicDBObject visit(After after) {
						return new BasicDBObject(MONGO_GREATER_THAN, Collections.min(values));
					}
				});
				return new BasicDBObject(fullyQualifiedPath(queryType, query.getAttribute()), rhs);
			}

			private List<Long> toMillis(List<DateTime> values) {
				return Lists.transform(values, new Function<DateTime, Long>(){
					@Override
					public Long apply(DateTime time) {
						return time.getMillis();
					}
				});
			}

			@Override
			public DBObject visit(ConjunctiveQuery query) {
				if (query.operands().size() == 1) {
					return buildQuery(Iterables.getOnlyElement(query.operands()), queryType);
				}
				BasicDBObject object = new BasicDBObject();
				for(ContentQuery subQuery : query.operands()) {
					object.putAll(buildQuery(subQuery, queryType));
				}
				return object;
			}

			@Override
			public BasicDBObject visit(MatchesNothing noOp) {
				throw new IllegalArgumentException();
			}
		});
	}
	
	private static BasicDBList list(Collection<?> elems) {
		final BasicDBList list = new BasicDBList();
		list.addAll(elems);
		return list;
	}
	
	private static String fullyQualifiedPath(Class<? extends Description> queryType, Attribute<?> attribute) {
		String entityPath = entityPath(queryType, attribute); 
		return entityPath + (entityPath.length() > 0 ? "." : "") +  attributeName(queryType, attribute);
	}

	private static String attributeName(Class<? extends Description> queryType, Attribute<?> attribute) {
		if  (!Playlist.class.equals(queryType) && Attributes.PLAYLIST_URI.equals(attribute)) {
			return "containedInUris";
		}
		if (Item.class.equals(queryType) && Attributes.ITEM_URI.equals(attribute)) {
			return "aliases";
		}
		if (Brand.class.equals(queryType) && Attributes.BRAND_URI.equals(attribute)) {
			return "aliases";
		}
		if (Playlist.class.equals(queryType) && Attributes.PLAYLIST_URI.equals(attribute)) {
            return "aliases";
        }
		return attribute.javaAttributeName() + (attribute.isCollectionOfValues() ? "s" : "");
	}

	private static String entityPath(Class<? extends Description> queryType, Attribute<?> attribute) {
		Class<? extends Description> entity = attribute.target();
		if (Item.class.isAssignableFrom(entity)) {
			return "";
		}
		if (Playlist.class.equals(entity)) {
			return "";
		}
		if (Brand.class.equals(entity)) {
			return Item.class.equals(queryType) ? "brand" : "";
		}
		if (Version.class.equals(entity)) {
			return "versions";
		}
		if (Broadcast.class.equals(entity)) {
			return "versions.broadcasts";
		}
		if (Encoding.class.equals(entity)) {
			return "versions.manifestedAs";
		}
		if (Location.class.equals(entity)) {
			return "versions.manifestedAs.availableAt";
		}
		throw new UnsupportedOperationException();
	}
}
