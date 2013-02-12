package org.atlasapi.persistence.content.elasticsearch.support;

import java.util.Date;
import java.util.List;
import java.util.Map;

import org.atlasapi.content.criteria.AtomicQuerySet;
import org.atlasapi.content.criteria.AttributeQuery;
import org.atlasapi.content.criteria.BooleanAttributeQuery;
import org.atlasapi.content.criteria.DateTimeAttributeQuery;
import org.atlasapi.content.criteria.EnumAttributeQuery;
import org.atlasapi.content.criteria.IdAttributeQuery;
import org.atlasapi.content.criteria.IntegerAttributeQuery;
import org.atlasapi.content.criteria.MatchesNothing;
import org.atlasapi.content.criteria.QueryVisitor;
import org.atlasapi.content.criteria.StringAttributeQuery;
import org.atlasapi.content.criteria.operator.BooleanOperatorVisitor;
import org.atlasapi.content.criteria.operator.DateTimeOperatorVisitor;
import org.atlasapi.content.criteria.operator.EnumOperatorVisitor;
import org.atlasapi.content.criteria.operator.IntegerOperatorVisitor;
import org.atlasapi.content.criteria.operator.Operators.After;
import org.atlasapi.content.criteria.operator.Operators.Before;
import org.atlasapi.content.criteria.operator.Operators.Beginning;
import org.atlasapi.content.criteria.operator.Operators.Equals;
import org.atlasapi.content.criteria.operator.Operators.GreaterThan;
import org.atlasapi.content.criteria.operator.Operators.LessThan;
import org.atlasapi.content.criteria.operator.StringOperatorVisitor;
import org.atlasapi.media.common.Id;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.joda.time.DateTime;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;

public class EsQueryBuilder {

    private static final Joiner PATH_JOINER = Joiner.on(".");

    public Map<String,Object> buildQuery(AtomicQuerySet operands) {
        List<Object> conjuncts = Lists.newLinkedList();
        for (Object atomBuilder : builders(operands).values()) {
            conjuncts.add(atomBuilder);
        }
        return ImmutableMap.<String,Object>of("bool", 
            ImmutableMap.<String,Object>of("must", conjuncts)
        );
    }

    private Map<String,Object> builders(AtomicQuerySet operands) {
        final Map<String,Object> queryBuilders = Maps.newHashMap();
        List<QueryBuilder> qbs = operands.accept(new QueryVisitor<QueryBuilder>() {

            @Override
            public QueryBuilder visit(final IntegerAttributeQuery query) {
                final String name = query.getAttributeName();
                final List<Integer> values = query.getValue();
                return query.accept(new IntegerOperatorVisitor<QueryBuilder>() {

                    @Override
                    public QueryBuilder visit(Equals equals) {
                        Object[] vals = values.toArray(new Object[values.size()]);
                        return QueryBuilders.termsQuery(name, vals);
                    }

                    @Override
                    public QueryBuilder visit(LessThan lessThan) {
                        return QueryBuilders.rangeQuery(name)
                            .lt(Ordering.natural().max(values));
                    }

                    @Override
                    public QueryBuilder visit(GreaterThan greaterThan) {
                        return QueryBuilders.rangeQuery(name)
                            .gt(Ordering.natural().min(values));
                    }
                });
            }

            @Override
            public QueryBuilder visit(StringAttributeQuery query) {
                final List<String> values = query.getValue();

                final String name = query.getAttributeName();
                QueryBuilder qb = query.accept(new StringOperatorVisitor<QueryBuilder>() {

                    @Override
                    public QueryBuilder visit(Equals equals) {
                        Object[] vals = values.toArray(new Object[values.size()]);
                        return QueryBuilders.termsQuery(name, vals);
                    }

                    @Override
                    public QueryBuilder visit(Beginning beginning) {
                        return QueryBuilders.prefixQuery(name, values.get(0));
                    }
                    
                });
                
                List<String> path = query.getAttribute().getPath();
                addQuery(path, 0, queryBuilders, query);
                
//                for(int i = 0; i < path.size(); i++) {
//                    Object existing = queryBuilders.get(path.get(i));
//                    if (existing == null) {
//                        if (i == path.size()-1) {//end
//                            queryBuilders.put(PATH_JOINER.join(path), );
//                        }
//                        queryBuilders.put("nested", value);
//                    } else {
//                        NestedQueryBuilder nqb = (NestedQueryBuilder) existing;
//                        nqb.
//                    }
//                }
//
//                for(int i = path.size()-2; i >= 0; i--) {
//                    String subPath = PATH_JOINER.join(path.subList(0, i+1));
//                    qb = QueryBuilders.nestedQuery(subPath, qb);
//                }
                return qb;
            }

            private Map<String, Object> addQuery(List<String> path, int i, Map<String, Object> queryBuilders,
                                  AttributeQuery<?> query) {
              Object existing = queryBuilders.get(path.get(i));
              if (existing == null) {
                  String name = query.getAttributeName();
                  if (i == path.size()-1) {
                      queryBuilders.put("terms",ImmutableMap.of(name, query.getValue()));
                  } else {
                      queryBuilders.put("nested", ImmutableMap.of(
                          "path", PATH_JOINER.join(path.subList(0, i+1)),
                          "query", addQuery(path, i+1, Maps.<String,Object>newHashMap(), query)
                      ));
                  }
              }
              return queryBuilders;
            }

            @Override
            public QueryBuilder visit(BooleanAttributeQuery query) {
                final String name = query.getAttributeName();
                final boolean value = query.getValue().get(0);
                return query.accept(new BooleanOperatorVisitor<QueryBuilder>() {
                    @Override
                    public QueryBuilder visit(Equals equals) {
                        return QueryBuilders.termQuery(name, value);
                    }
                });
            }

            @Override
            public QueryBuilder visit(EnumAttributeQuery<?> query) {
                final String name = query.getAttributeName();
                final List<?> values = query.getValue();
                return query.accept(new EnumOperatorVisitor<QueryBuilder>() {

                    @Override
                    public QueryBuilder visit(Equals equals) {
                        Object[] vals = values.toArray(new Object[values.size()]);
                        return QueryBuilders.termsQuery(name, vals);
                    }
                });
            }

            @Override
            public QueryBuilder visit(DateTimeAttributeQuery query) {
                final String name = query.getAttributeName();
                final List<Date> values = toDates(query.getValue());
                return query.accept(new DateTimeOperatorVisitor<QueryBuilder>() {

                    @Override
                    public QueryBuilder visit(Before before) {
                        return QueryBuilders.rangeQuery(name)
                            .lt(Ordering.natural().max(values));
                    }

                    @Override
                    public QueryBuilder visit(After after) {
                        return QueryBuilders.rangeQuery(name)
                            .gt(Ordering.natural().min(values));
                    }

                    @Override
                    public QueryBuilder visit(Equals equals) {
                        return QueryBuilders.termsQuery(name, values);
                    }
                });
            }

            private List<Date> toDates(List<DateTime> value) {
                return Lists.transform(value, new Function<DateTime, Date>() {
                    @Override
                    public Date apply(DateTime input) {
                        return input.toDate();
                    }
                });
            }

            @Override
            public QueryBuilder visit(MatchesNothing noOp) {
                throw new IllegalArgumentException();
            }

            @Override
            public QueryBuilder visit(IdAttributeQuery query) {
                final String name = query.getAttributeName();
                final List<Long> value = Lists.transform(query.getValue(), Id.toLongValue());
                
                List<String> path = query.getAttribute().getPath();
                addQuery(path, 0, queryBuilders, query);
                
                return query.accept(new IntegerOperatorVisitor<QueryBuilder>() {

                    @Override
                    public QueryBuilder visit(Equals equals) {
                        Object[] values = value.toArray(new Object[value.size()]);
                        return QueryBuilders.termsQuery(name, values);
                    }

                    @Override
                    public QueryBuilder visit(LessThan lessThan) {
                        return QueryBuilders.rangeQuery(name)
                            .lt(Ordering.natural().max(value));
                    }

                    @Override
                    public QueryBuilder visit(GreaterThan greaterThan) {
                        return QueryBuilders.rangeQuery(name)
                            .gt(Ordering.natural().min(value));
                    }
                });
            }
        });
        System.out.println("++Map");
        System.out.println(queryBuilders);
        System.out.println("--Map");
        return queryBuilders;
    }
    
}
