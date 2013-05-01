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

import static com.metabroadcast.common.persistence.mongo.MongoConstants.GREATER_THAN;
import static com.metabroadcast.common.persistence.mongo.MongoConstants.IN;
import static com.metabroadcast.common.persistence.mongo.MongoConstants.LESS_THAN;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.regex.Pattern;

import org.atlasapi.content.criteria.BooleanAttributeQuery;
import org.atlasapi.content.criteria.ContentQuery;
import org.atlasapi.content.criteria.DateTimeAttributeQuery;
import org.atlasapi.content.criteria.EnumAttributeQuery;
import org.atlasapi.content.criteria.FloatAttributeQuery;
import org.atlasapi.content.criteria.IdAttributeQuery;
import org.atlasapi.content.criteria.IntegerAttributeQuery;
import org.atlasapi.content.criteria.MatchesNothing;
import org.atlasapi.content.criteria.QueryVisitor;
import org.atlasapi.content.criteria.StringAttributeQuery;
import org.atlasapi.content.criteria.attribute.Attribute;
import org.atlasapi.content.criteria.operator.ComparableOperatorVisitor;
import org.atlasapi.content.criteria.operator.DateTimeOperatorVisitor;
import org.atlasapi.content.criteria.operator.EqualsOperatorVisitor;
import org.atlasapi.content.criteria.operator.Operators.After;
import org.atlasapi.content.criteria.operator.Operators.Before;
import org.atlasapi.content.criteria.operator.Operators.Beginning;
import org.atlasapi.content.criteria.operator.Operators.Equals;
import org.atlasapi.content.criteria.operator.Operators.GreaterThan;
import org.atlasapi.content.criteria.operator.Operators.LessThan;
import org.atlasapi.content.criteria.operator.StringOperatorVisitor;
import org.atlasapi.media.common.Id;
import org.atlasapi.media.content.Container;
import org.atlasapi.media.content.Content;
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Encoding;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Location;
import org.atlasapi.media.entity.Policy;
import org.atlasapi.media.entity.Version;
import org.atlasapi.media.topic.Topic;
import org.joda.time.DateTime;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.metabroadcast.common.persistence.mongo.MongoConstants;
import com.metabroadcast.common.time.DateTimeZones;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

class MongoDBQueryBuilder {

	private static final Joiner DOTTED_MONGO_ATTRIBUTE_PATH = Joiner.on(".");

	DBObject buildQuery(ContentQuery query) {
		
		// handle attributes that are not part of a list structure
		Multimap<List<String>, ConstrainedAttribute> attributeConstraints = HashMultimap.create();
		for (ConstrainedAttribute constraint : buildQueries(query)) {
			if (constraint == null) {
				continue;
			}
			attributeConstraints.put(entityPath(constraint.attribute), constraint);
		}

		// sort the keys by length so that versions are dealt with before broadcasts etc.
		TreeMap<List<String>, Collection<ConstrainedAttribute>> map = Maps.newTreeMap(LENGTH_ORDER);
		map.putAll(attributeConstraints.asMap());
		
		DBObject finalQuery = new BasicDBObject();

		Map<List<String>, DBObject> queries = Maps.newHashMap();
		for (Entry<List<String>, Collection<ConstrainedAttribute>> entry : map.entrySet()) {
			
			List<String> entityPath = entry.getKey();
			
			Collection<ConstrainedAttribute> constraints = entry.getValue();
			
			if (entityPath.isEmpty()) {
				finalQuery.putAll(buildQueryForSingleLevelEntity(constraints));
				continue;
			} 
			
			DBObject parentDbObject = null;
				
			List<String> parentPath = entityPath;
			while(!parentPath.isEmpty()) {
				parentPath = parentPath.subList(0, parentPath.size() - 1);
				if (queries.get(parentPath) != null) {
					parentDbObject = queries.get(parentPath);
					break;
				}
			}
			if (parentDbObject == null) {
				parentDbObject = finalQuery;
				parentPath = ImmutableList.of();
			}
			
			DBObject rhs = buildQueryForSingleLevelEntity(constraints);
			String key = DOTTED_MONGO_ATTRIBUTE_PATH.join(entityPath.subList(parentPath.size(), entityPath.size()));
			DBObject attrObj = new BasicDBObject(key, new BasicDBObject(MongoConstants.ELEM_MATCH,  rhs));
			parentDbObject.putAll(attrObj);
			queries.put(entityPath, rhs);
		}
		return finalQuery;
	}

	private DBObject buildQueryForSingleLevelEntity(Collection<ConstrainedAttribute> contraints) {
		DBObject rhs = new BasicDBObject();
		for (ConstrainedAttribute constrainedAttribute : contraints) {
			String name = attributeName(constrainedAttribute.attribute);
			if (rhs.containsField(name)) {
				((DBObject) rhs.get(name)).putAll((DBObject) constrainedAttribute.queryOrValue());
			} else {
				rhs.put(name, constrainedAttribute.queryOrValue());
			}
		}
		return rhs;
	}

	private List<ConstrainedAttribute> buildQueries(ContentQuery query) {
		return query.accept(new QueryVisitor<ConstrainedAttribute>() {

			@Override
			@SuppressWarnings("unchecked")
			public ConstrainedAttribute visit(IntegerAttributeQuery query) {
				final List<Integer> values = (List<Integer>) query.getValue();
				
				BasicDBObject rhs = query.accept(new ComparableOperatorVisitor<BasicDBObject>() {

					@Override
					public BasicDBObject visit(Equals equals) {
						return new BasicDBObject(IN, list(values));
					}

					@Override
					public BasicDBObject visit(LessThan lessThan) {
						return new BasicDBObject(LESS_THAN, Collections.max(values));
					}

					@Override
					public BasicDBObject visit(GreaterThan greaterThan) {
						return new BasicDBObject(GREATER_THAN, Collections.min(values));
					}
				});
				
				return new ConstrainedAttribute(query.getAttribute(), rhs);
			}

			@Override
			public ConstrainedAttribute visit(final StringAttributeQuery query) {
				final BasicDBList values = new BasicDBList();
				values.addAll(query.getValue());
				
				return query.accept(new StringOperatorVisitor<ConstrainedAttribute>() {

					@Override
					public ConstrainedAttribute visit(Equals equals) {
						return new ConstrainedAttribute(query.getAttribute(), new BasicDBObject(IN, values));
					}

					@Override
					public ConstrainedAttribute visit(Beginning beginning) {
						Pattern pattern = Pattern.compile("^" + (String) query.getValue().get(0), Pattern.CASE_INSENSITIVE);
						return new ConstrainedAttribute(query.getAttribute(), pattern);
					}
				});
				
			}

			@Override
			public ConstrainedAttribute visit(final BooleanAttributeQuery query) {
				
				if (query.isUnconditionallyTrue()) {
					return null;
				}
				
				final Boolean value = (Boolean) query.getValue().get(0);
				
				return query.accept(new EqualsOperatorVisitor<ConstrainedAttribute>() {

					@Override
					public ConstrainedAttribute visit(Equals equals) {
						return new ConstrainedAttribute(query.getAttribute(), value);
					}
				});
				
			}

			@Override
			@SuppressWarnings("unchecked")
			public ConstrainedAttribute visit(final EnumAttributeQuery<?> query) {
				
				final List<Enum<?>> values = (List<Enum<?>>) query.getValue();
				
				return query.accept(new EqualsOperatorVisitor<ConstrainedAttribute>() {

					@Override
					public ConstrainedAttribute visit(Equals equals) {
						return new ConstrainedAttribute(query.getAttribute(), new BasicDBObject(IN, list(toLowercaseStrings(values))));
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
			public ConstrainedAttribute visit(DateTimeAttributeQuery query) {
				final List<Date> values = toDate((List<DateTime>) query.getValue());

				BasicDBObject rhs = query.accept(new DateTimeOperatorVisitor<BasicDBObject>() {

					@Override
					public BasicDBObject visit(Before before) {
						return new BasicDBObject(LESS_THAN, Collections.max(values));
					}

					@Override
					public BasicDBObject visit(After after) {
						return new BasicDBObject(GREATER_THAN, Collections.min(values));
					}

					@Override
					public BasicDBObject visit(Equals equals) {
						throw new UnsupportedOperationException();
					}
				});
				return new ConstrainedAttribute(query.getAttribute(), rhs);
			}

			private List<Date> toDate(List<DateTime> values) {
				return Lists.transform(values, new Function<DateTime, Date>(){
					@Override
					public Date apply(DateTime time) {
						return time.toDateTime(DateTimeZones.UTC).toDate();
					}
				});
			}

			@Override
			public ConstrainedAttribute visit(MatchesNothing noOp) {
				throw new IllegalArgumentException();
			}
			
			@Override
			public ConstrainedAttribute visit(final IdAttributeQuery query) {
			    final List<Long> values = Lists.transform(query.getValue(), Id.toLongValue());
			    return query.accept(new ComparableOperatorVisitor<ConstrainedAttribute>() {

                    @Override
                    public ConstrainedAttribute visit(Equals equals) {
                        return new ConstrainedAttribute(query.getAttribute(), new BasicDBObject(IN, list(values)));
                    }

                    private Collection<?> toLowercaseStrings(Collection<Enum<?>> values) {
                        List<String> strings = Lists.newArrayList();
                        for (Enum<?> value : values) {
                            strings.add(value.toString().toLowerCase());
                        }
                        return strings;
                    }

                    @Override
                    public ConstrainedAttribute visit(LessThan lessThan) {
                        // TODO Auto-generated method stub
                        return null;
                    }

                    @Override
                    public ConstrainedAttribute visit(GreaterThan greaterThan) {
                        // TODO Auto-generated method stub
                        return null;
                    }
                });
			}
			
			@Override
			public ConstrainedAttribute visit(FloatAttributeQuery query) {
			    throw new UnsupportedOperationException();
			}
		});
	}
	
	private static BasicDBList list(Collection<?> elems) {
		final BasicDBList list = new BasicDBList();
		list.addAll(elems);
		return list;
	}

	private static String attributeName(Attribute<?> attribute) {
		if (Policy.class.equals(attribute.target())) {
			return "policy." + attribute.javaAttributeName();
		}
		return attribute.javaAttributeName() + ((attribute.isCollectionOfValues()  && !attribute.javaAttributeName().endsWith("s")) ? "s" : "");
	}

	private static List<String> entityPath(Attribute<?> attribute) {
		Class<? extends Identified> entity = attribute.target();
		if (Topic.class.isAssignableFrom(entity)) {
		    return ImmutableList.of();
		}
		if (Item.class.isAssignableFrom(entity)) {
			return ImmutableList.of("contents");
		}
		if (Container.class.isAssignableFrom(entity)) {
			return ImmutableList.of();
		}
		if (Version.class.equals(entity)) {
			return ImmutableList.of("contents", "versions");
		}
		if (Broadcast.class.equals(entity)) {
			return ImmutableList.of("contents", "versions", "broadcasts");
		}
		if (Encoding.class.equals(entity)) {
			return ImmutableList.of("contents", "versions", "manifestedAs");
		}
		if (Location.class.equals(entity)) {
			return ImmutableList.of("contents", "versions", "manifestedAs", "availableAt");
		}
		if (Policy.class.equals(entity)) {
			return ImmutableList.of("contents", "versions", "manifestedAs", "availableAt");
		}
		if (Content.class.equals(entity)) {
			return ImmutableList.of();
		}
		throw new UnsupportedOperationException();
	}
	
	private static class ConstrainedAttribute {
		
		private final Attribute<?> attribute;
		private final DBObject query;
		private final Object shouldEqual;

		private ConstrainedAttribute(Attribute<?> attribute, BasicDBObject query) {
			this.attribute = attribute;
			this.query = query;
			this.shouldEqual = null;
		}

		public Object queryOrValue() {
			return shouldEqual == null ? query : shouldEqual;
		}

		
		private <T> ConstrainedAttribute(Attribute<T> attribute, T shouldEqual) {
			this.attribute = attribute;
			this.shouldEqual = shouldEqual;
			this.query = null;
		}
		
		private ConstrainedAttribute(Attribute<String> attribute, Pattern shouldEqual) {
			this.attribute = attribute;
			this.shouldEqual = shouldEqual;
			this.query = null;
		}
	}
	
	private static final Joiner NO_SPACES = Joiner.on("");

	private static final Comparator<List<String>> LENGTH_ORDER = new Comparator<List<String>>() {

		@Override
		public int compare(List<String> o1, List<String> o2) {
			int lengthComparison = Integer.valueOf(o1.size()).compareTo(o2.size());
			if (lengthComparison == 0) {
				return NO_SPACES.join(o1).compareTo(NO_SPACES.join(o2));
			}
			return lengthComparison;
		}
	};
}
