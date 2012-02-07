package org.atlasapi.persistence.content.mongo;

import static com.metabroadcast.common.persistence.mongo.MongoBuilders.sort;
import static com.metabroadcast.common.persistence.mongo.MongoBuilders.where;
import static com.metabroadcast.common.persistence.mongo.MongoConstants.ID;
import static org.atlasapi.persistence.content.ContentCategory.CHILD_ITEM;
import static org.atlasapi.persistence.content.ContentCategory.CONTAINER;
import static org.atlasapi.persistence.content.ContentCategory.PROGRAMME_GROUP;
import static org.atlasapi.persistence.content.ContentCategory.TOP_LEVEL_ITEM;

import java.util.Iterator;
import java.util.List;

import org.atlasapi.content.criteria.ContentQuery;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentCategory;
import org.atlasapi.persistence.content.listing.ContentLister;
import org.atlasapi.persistence.content.listing.ContentListingCriteria;
import org.atlasapi.persistence.media.entity.ContainerTranslator;
import org.atlasapi.persistence.media.entity.ItemTranslator;
import org.atlasapi.persistence.topic.TopicContentLister;
import org.joda.time.DateTime;

import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.persistence.mongo.MongoConstants;
import com.metabroadcast.common.persistence.mongo.MongoQueryBuilder;
import com.metabroadcast.common.persistence.mongo.MongoSortBuilder;
import com.mongodb.Bytes;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoException.CursorNotFound;

public class MongoContentLister implements ContentLister, LastUpdatedContentFinder, TopicContentLister {

    private final ContainerTranslator containerTranslator;
    private final ItemTranslator itemTranslator;

    private final MongoContentTables contentTables;

    public MongoContentLister(DatabasedMongo mongo) {
        this.contentTables = new MongoContentTables(mongo);
        SubstitutionTableNumberCodec idCodec = new SubstitutionTableNumberCodec();
        this.containerTranslator = new ContainerTranslator(idCodec);
        this.itemTranslator = new ItemTranslator(idCodec);
    }
    
    @Override
    public Iterator<Content> listContent(ContentListingCriteria criteria) {
        List<Publisher> publishers = remainingPublishers(criteria);
        
        if(publishers.isEmpty()) {
            return Iterators.emptyIterator();
        }

        return iteratorsFor(publishers, criteria);
    }
    

    private Iterator<Content> iteratorsFor(final List<Publisher> publishers, ContentListingCriteria criteria) {
        final String uri = criteria.getProgress().getUri();
        final List<ContentCategory> initialCats = remainingTables(criteria);
        final List<ContentCategory> allCats = criteria.getCategories();
        
        return Iterators.concat(Iterators.transform(publishers.iterator(), new Function<Publisher, Iterator<Content>>() {
            @Override
            public Iterator<Content> apply(final Publisher publisher) {
                return contentIterator(first(publisher, publishers) ? initialCats : allCats, new ListingCursorBuilder() {
                    
                    public DBObject queryForCategory(ContentCategory category) {
                        MongoQueryBuilder query = queryCriteria(publisher);
                        if(first(publisher, publishers) && first(category, initialCats) && !Strings.isNullOrEmpty(uri) ) {
                            query.fieldGreaterThan(ID, uri);
                        }
                        return query.build();
                    }

                    @Override
                    public DBCursor cursorFor(ContentCategory category) {

                        return cursorFor(category, queryForCategory(category));
                    }

					@Override
					public DBCursor cursorForGreaterThan(ContentCategory category, Content greaterThan) {
						return cursorFor(category, queryCriteria(publisher).fieldGreaterThan(ID, greaterThan.getCanonicalUri()).build());
					}
					
					private DBCursor cursorFor(ContentCategory category, DBObject query) {
						return contentTables.collectionFor(category)
                                .find(query)
                                .batchSize(100)
                                .sort(new MongoSortBuilder().ascending("publisher").ascending(MongoConstants.ID).build())
                                .addOption(Bytes.QUERYOPTION_NOTIMEOUT);
                    }

					private MongoQueryBuilder queryCriteria(final Publisher publisher) {
					    return where().fieldEquals("publisher", publisher.key());
					}

                });
                
            }
            private <T> boolean first(T e, List<T> es) {
                return e.equals(es.get(0));
            }
        }));
    }
    

    private List<Publisher> remainingPublishers(ContentListingCriteria criteria) {
        List<Publisher> publishers = criteria.getPublishers().isEmpty() ? ImmutableList.copyOf(Publisher.values()) : criteria.getPublishers();
        Publisher currentPublisher = criteria.getProgress().getPublisher();
        
        return publishers.subList(currentPublisher == null ? 0 : publishers.indexOf(currentPublisher), publishers.size());
    }

    private List<ContentCategory> remainingTables(ContentListingCriteria criteria) {
        List<ContentCategory> tables = criteria.getCategories().isEmpty() ? ImmutableList.copyOf(ContentCategory.values()) : criteria.getCategories();
        ContentCategory currentTable = criteria.getProgress().getCategory();
        
        return tables.subList(currentTable == null ? 0 : tables.indexOf(currentTable), tables.size());
    }
    
    private static final List<ContentCategory> BRAND_SERIES_AND_ITEMS_TABLES = ImmutableList.of(CONTAINER, PROGRAMME_GROUP, TOP_LEVEL_ITEM, CHILD_ITEM);
    
    @Override
    public Iterator<Content> updatedSince(final Publisher publisher, final DateTime when) {
        return contentIterator(BRAND_SERIES_AND_ITEMS_TABLES, new ListingCursorBuilder() {
            @Override
            public DBCursor cursorFor(ContentCategory category) {
                return cursorFor(category, queryCriteria());
            }

            @Override
            public DBCursor cursorForGreaterThan(ContentCategory category, Content greaterThan) {
            	return cursorFor(category, queryCriteria()
            					.fieldGreaterThanOrEqualTo("publisher", greaterThan.getPublisher().key())
            					.fieldGreaterThan("thisOrChildLastUpdated", greaterThan.getThisOrChildLastUpdated()));
            				
            }
            
            private DBCursor cursorFor(ContentCategory category, MongoQueryBuilder queryCriteria) {
            	return contentTables.collectionFor(category)
            			.find(queryCriteria.build())
            			.sort(sort().ascending("publisher").ascending("thisOrChildLastUpdated").build());
            }

			private MongoQueryBuilder queryCriteria() {
				return where().fieldEquals("publisher", publisher.key()).fieldAfter("thisOrChildLastUpdated", when);
			}
            
        });
    }

    private Iterator<Content> contentIterator(final List<ContentCategory> tables, final ListingCursorBuilder cursorBuilder) {
        return new AbstractIterator<Content>() {

            private final Iterator<ContentCategory> tablesIt = tables.iterator();
            private Iterator<DBObject> currentResults = Iterators.emptyIterator();
            private Function<DBObject, ? extends Content> currentTranslator;
            private Content currentContent = null;
            private ContentCategory currentCategory = null;
            
            @Override
            protected Content computeNext() {
            	boolean hasNext;
            	try {
            		hasNext = currentResults.hasNext();
            	}
            	catch(CursorNotFound e) {
            		currentResults = cursorBuilder.cursorForGreaterThan(currentCategory, currentContent);
            		hasNext = currentResults.hasNext();
            	}
            	
                while (!hasNext) {
                    if (!tablesIt.hasNext()) {
                        return endOfData();
                    }
                    currentCategory = tablesIt.next();
                    currentTranslator = TRANSLATORS.get(currentCategory);
                    currentResults = cursorBuilder.cursorFor(currentCategory);
                    hasNext = currentResults.hasNext();
                }           
                currentContent = currentTranslator.apply(currentResults.next());
                return currentContent;
            }
        };
    }

    private final Function<DBObject, Container> TO_CONTAINER = new Function<DBObject, Container>() {
        @Override
        public Container apply(DBObject input) {
            return containerTranslator.fromDBObject(input, null);
        }
    };
    
    private final Function<DBObject, Item> TO_ITEM = new Function<DBObject, Item>() {
        @Override
        public Item apply(DBObject input) {
            return itemTranslator.fromDBObject(input, null);
        }
    };

    private final ImmutableMap<ContentCategory, Function<DBObject, ? extends Content>> TRANSLATORS = ImmutableMap.<ContentCategory, Function<DBObject, ? extends Content>>of(
            CHILD_ITEM, TO_ITEM, 
            PROGRAMME_GROUP, TO_CONTAINER, 
            TOP_LEVEL_ITEM, TO_ITEM, 
            CONTAINER, TO_CONTAINER);

    private interface ListingCursorBuilder {
        
        DBCursor cursorFor(ContentCategory category);
        DBCursor cursorForGreaterThan(ContentCategory categtory, Content startingAt);
        
    }
    
    @Override
    //TODO: enable use of contentQuery?
    public Iterator<Content> contentForTopic(final Long topicId, ContentQuery contentQuery) {
        return contentIterator(BRAND_SERIES_AND_ITEMS_TABLES, new ListingCursorBuilder() {
            @Override
            public DBCursor cursorFor(ContentCategory category) {
                return cursorFor(category, queryCriteria());
            }
            
            @Override
            public DBCursor cursorForGreaterThan(ContentCategory category, Content greaterThan) {
            	return cursorFor(category, queryCriteria().fieldGreaterThan(ID, greaterThan.getCanonicalUri()));
            }

            private DBCursor cursorFor(ContentCategory category, MongoQueryBuilder query) {
            	return contentTables.collectionFor(category).find(query.build()).sort(sort().ascending(ID).build());
            }

			private MongoQueryBuilder queryCriteria() {
				return where().fieldEquals("topics.topic", topicId);
			}
            
        });
    }

}
