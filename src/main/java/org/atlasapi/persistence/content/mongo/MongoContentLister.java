package org.atlasapi.persistence.content.mongo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.atlasapi.content.criteria.ContentQuery;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentCategory;
import org.atlasapi.persistence.content.KnownTypeContentResolver;
import org.atlasapi.persistence.content.listing.ContentLister;
import org.atlasapi.persistence.content.listing.ContentListingCriteria;
import org.atlasapi.persistence.content.listing.SelectedContentLister;
import org.atlasapi.persistence.event.EventContentLister;
import org.atlasapi.persistence.media.entity.ContainerTranslator;
import org.atlasapi.persistence.media.entity.DescribedTranslator;
import org.atlasapi.persistence.media.entity.ItemTranslator;
import org.atlasapi.persistence.topic.TopicContentLister;

import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.persistence.mongo.MongoConstants;
import com.metabroadcast.common.persistence.mongo.MongoQueryBuilder;
import com.metabroadcast.common.persistence.mongo.MongoSelectBuilder;
import com.metabroadcast.common.persistence.mongo.MongoSortBuilder;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;

import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.Bytes;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.metabroadcast.common.persistence.mongo.MongoBuilders.sort;
import static com.metabroadcast.common.persistence.mongo.MongoBuilders.where;
import static com.metabroadcast.common.persistence.mongo.MongoConstants.ID;
import static org.atlasapi.persistence.content.ContentCategory.CHILD_ITEM;
import static org.atlasapi.persistence.content.ContentCategory.CONTAINER;
import static org.atlasapi.persistence.content.ContentCategory.PROGRAMME_GROUP;
import static org.atlasapi.persistence.content.ContentCategory.TOP_LEVEL_ITEM;

public class MongoContentLister implements ContentLister, LastUpdatedContentFinder,
        TopicContentLister, EventContentLister, SelectedContentLister {

    private static final Logger log = LoggerFactory.getLogger(MongoContentLister.class);

    private static final Long NULL_ID = null;
    private static final Publisher NULL_PUBLISHER = null;


    private final ContainerTranslator containerTranslator;
    private final ItemTranslator itemTranslator;

    private final MongoContentTables contentTables;
    private final KnownTypeContentResolver contentResolver;

    public MongoContentLister(DatabasedMongo mongo, KnownTypeContentResolver contentResolver) {
        this.contentTables = new MongoContentTables(mongo);
        SubstitutionTableNumberCodec idCodec = new SubstitutionTableNumberCodec();
        this.containerTranslator = new ContainerTranslator(idCodec);
        this.itemTranslator = new ItemTranslator(idCodec);
        this.contentResolver = checkNotNull(contentResolver);
    }

    @Override
    public Iterator<Content> listContent(ContentListingCriteria criteria){
        List<Publisher> publishers = remainingPublishers(criteria);

        if(publishers.isEmpty()) {
            return Iterators.emptyIterator();
        }

        return iteratorsFor(publishers, criteria);
    }

    @Override
    public Iterator<String> listContentUris(ContentListingCriteria criteria) {
        List<Publisher> publishers = remainingPublishers(criteria);

        if(publishers.isEmpty()) {
            return Iterators.emptyIterator();
        }

        Iterator<Content> contentIterator = iteratorsFor(publishers, criteria, true);
        Iterator<String> uriIterator = Iterators.transform(contentIterator,
                Identified::getCanonicalUri
        );
        return uriIterator;
    }

    private Iterator<Content> iteratorsFor(final List<Publisher> publishers,
            ContentListingCriteria criteria) {

        return iteratorsFor(publishers, criteria, false);
    }

    private Iterator<Content> iteratorsFor(final List<Publisher> publishers,
            ContentListingCriteria criteria, boolean fetchOnlyUris) {

        final String uri = criteria.getProgress().getUri();
        final List<ContentCategory> initialCats = remainingTables(criteria);
        final List<ContentCategory> allCats = criteria.getCategories();

        return Iterators.concat(Iterators.transform(publishers.iterator(), new Function<Publisher, Iterator<Content>>() {
            @Override
            public Iterator<Content> apply(final Publisher publisher) {
                return contentIterator(first(publisher, publishers) ? initialCats : allCats, new ListingCursorBuilder<Content>() {

                    public DBObject queryForCategory(ContentCategory category, boolean fetchOnlyUris) {
                        MongoQueryBuilder query;
                        //if true, then select only the URis from all content from a publisher; goal
                        //is to get an Iterator<Content> filled only with the _id field (uris); by
                        //then preloading into a list, we try and prevent MongoCursorTimeout exception
                        if(fetchOnlyUris) {
                            query = where().fieldEquals("publisher", publisher.key())
                                    .selecting(new MongoSelectBuilder().field("_id"));
                        }
                        else {
                            query = where().fieldEquals("publisher", publisher.key());
                        }
                        if(first(publisher, publishers) && first(category, initialCats) && !Strings.isNullOrEmpty(uri) ) {
                            query.fieldGreaterThan(ID, uri);
                        }
                        return query.build();
                    }

                    @Override
                    public DBCursor cursorFor(ContentCategory category) {
                        return contentTables.collectionFor(category)
                                .find(queryForCategory(category, fetchOnlyUris))
                                .batchSize(100)
                                .sort(new MongoSortBuilder().ascending("publisher").ascending(MongoConstants.ID).build())
                                .noCursorTimeout(true);
                    }

                    @Override
                    public Function<DBObject, Content> translatorFor(ContentCategory contentCategory) {
                        return toContentFunction(contentCategory);
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
        return contentIterator(BRAND_SERIES_AND_ITEMS_TABLES, new ListingCursorBuilder<Content>() {
            @Override
            public DBCursor cursorFor(ContentCategory category) {
                return contentTables.collectionFor(category)
                            .find(where().fieldEquals("publisher", publisher.key()).fieldAfter("thisOrChildLastUpdated", when).build())
                            .sort(sort().ascending("publisher").ascending("thisOrChildLastUpdated").build())
                            .batchSize(100)
                            .noCursorTimeout(true);
            }

            @Override
            public Function<DBObject, Content> translatorFor(ContentCategory contentCategory) {
                return toContentFunction(contentCategory);
            }
        });
    }

    @Override
    public Iterator<Content> updatedBetween(final Publisher publisher, final DateTime from, final DateTime to) {
        return contentIterator(BRAND_SERIES_AND_ITEMS_TABLES, new ListingCursorBuilder<Content>() {
            @Override
            public DBCursor cursorFor(ContentCategory category) {
                return contentTables.collectionFor(category)
                        .find(where()
                                .fieldEquals("publisher", publisher.key())
                                .inclusiveRange("thisOrChildLastUpdated", from, to)
                                .build())
                        .sort(sort().ascending("publisher").ascending("thisOrChildLastUpdated").build())
                        .batchSize(100)
                        .noCursorTimeout(true);
            }

            @Override
            public Function<DBObject, Content> translatorFor(ContentCategory contentCategory) {
                return toContentFunction(contentCategory);
            }
        });
    }

    private <T> Iterator<T> contentIterator(final List<ContentCategory> tables, final ListingCursorBuilder<T> cursorBuilder) {
        return new AbstractIterator<T>() {

            private final Iterator<ContentCategory> tablesIt = tables.iterator();
            private Iterator<DBObject> currentResults = Iterators.emptyIterator();
            private Function<DBObject, T> currentTranslator;

            @Override
            protected T computeNext() {
                while (!currentResults.hasNext()) {
                    if (!tablesIt.hasNext()) {
                        return endOfData();
                    }
                    ContentCategory table = tablesIt.next();
                    currentTranslator = cursorBuilder.translatorFor(table);
                    if (currentTranslator == null) {
                       log.error("No translator found for content category " + table.toString());
                    }
                    currentResults = cursorBuilder.cursorFor(table);
                }
                return currentTranslator.apply(currentResults.next());
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

    @Override
    public Iterator<Content> contentForEvent(final List<Long> eventIds, ContentQuery query) {
        List<ContentCategory> itemTables = ImmutableList.of(TOP_LEVEL_ITEM);
        Iterator<LookupRef> allCanonicalUris = contentIterator(itemTables,
                new ListingCursorBuilder<LookupRef>() {

                    @Override
                    public DBCursor cursorFor(ContentCategory category) {
                        return contentTables.collectionFor(category).find(
                                where().longFieldIn("events._id", eventIds).build(),
                                BasicDBObjectBuilder.start(MongoConstants.ID, 1).get()
                        ).sort(sort().ascending(ID).build());
                    }

                    @Override
                    public Function<DBObject, LookupRef> translatorFor(ContentCategory contentCategory) {
                        return toLookupRef(contentCategory);
                    }
                });
        Iterable<LookupRef> selectionToResolve = query.getSelection().applyTo(allCanonicalUris);
        return Iterables
                .filter(
                        contentResolver.findByLookupRefs(selectionToResolve).getAllResolvedResults(),
                        Content.class
                ).iterator();
    }

    private interface ListingCursorBuilder<T> {

        DBCursor cursorFor(ContentCategory category);
        Function<DBObject, T> translatorFor(ContentCategory contentCategory);

    }

    @Override
    //TODO: enable use of contentQuery?
    public Iterator<Content> contentForTopic(final Long topicId, ContentQuery contentQuery) {
        Iterator<LookupRef> allCanonicalUris = contentIterator(BRAND_SERIES_AND_ITEMS_TABLES, new ListingCursorBuilder<LookupRef>() {
            @Override
            public DBCursor cursorFor(ContentCategory category) {
                return contentTables.collectionFor(category).find(
                        where().fieldEquals("topics.topic", topicId)
                               .fieldNotEqualTo(DescribedTranslator.ACTIVELY_PUBLISHED_KEY, false)
                               .build(),
                        BasicDBObjectBuilder.start(MongoConstants.ID, 1).get()
                        ).sort(sort().ascending(ID).build());
            }

            @Override
            public Function<DBObject, LookupRef> translatorFor(ContentCategory contentCategory) {
                return toLookupRef(contentCategory);
            }
        });
        Iterable<LookupRef> selectionToResolve = contentQuery.getSelection().applyTo(allCanonicalUris);

        return Iterables
                .filter(
                        contentResolver.findByLookupRefs(selectionToResolve).getAllResolvedResults(),
                        Content.class
                       ).iterator();
    }

    private Function<DBObject, LookupRef> toLookupRef(final ContentCategory category) {
        return new Function<DBObject, LookupRef>() {

            @Override
            public LookupRef apply(DBObject input) {
                String uri = TranslatorUtils.toString(input, MongoConstants.ID);
                return new LookupRef(uri, NULL_ID, NULL_PUBLISHER, category);
            }
        };
    }

    private Function<DBObject, Content> toContentFunction(ContentCategory category) {
        return (Function<DBObject, Content>) TRANSLATORS.get(category);
    }
}
