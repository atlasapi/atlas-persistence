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
import org.atlasapi.media.content.Container;
import org.atlasapi.media.content.Content;
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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.atlasapi.persistence.bootstrap.ContentBootstrapper;

public class MongoContentLister implements ContentLister, LastUpdatedContentFinder, TopicContentLister {

    private static final Log log = LogFactory.getLog(MongoContentLister.class);
    //
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

        if (publishers.isEmpty()) {
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
                        MongoQueryBuilder query = where().fieldEquals("publisher", publisher.key());
                        if (first(publisher, publishers) && first(category, initialCats) && !Strings.isNullOrEmpty(uri)) {
                            query.fieldGreaterThan(ID, uri);
                        }
                        return query.build();
                    }

                    @Override
                    public DBCursor cursorFor(ContentCategory category) {
                        return contentTables.collectionFor(category).find(queryForCategory(category)).batchSize(100).sort(new MongoSortBuilder().ascending("publisher").ascending(MongoConstants.ID).build()).addOption(Bytes.QUERYOPTION_NOTIMEOUT);
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
                return contentTables.collectionFor(category).find(where().fieldEquals("publisher", publisher.key()).fieldAfter("thisOrChildLastUpdated", when).build()).sort(sort().ascending("publisher").ascending("thisOrChildLastUpdated").build());
            }
        });
    }

    private Iterator<Content> contentIterator(final List<ContentCategory> tables, final ListingCursorBuilder cursorBuilder) {
        return new AbstractIterator<Content>() {

            private final Iterator<ContentCategory> tablesIt = tables.iterator();
            private Iterator<DBObject> currentResults = Iterators.emptyIterator();
            private Function<DBObject, ? extends Content> currentTranslator;

            @Override
            protected Content computeNext() {
                while (!currentResults.hasNext()) {
                    if (!tablesIt.hasNext()) {
                        return endOfData();
                    }
                    ContentCategory table = tablesIt.next();
                    currentTranslator = TRANSLATORS.get(table);
                    currentResults = cursorBuilder.cursorFor(table);
                }
                Content translated = null;
                do {
                    try {
                        translated = currentTranslator.apply(currentResults.next());
                    } catch (Exception ex) {
                        log.warn(ex.getMessage(), ex);
                    }
                } while (translated == null && currentResults.hasNext());
                if (translated != null) {
                    return translated;
                } else {
                    return computeNext();
                }
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
    }

    @Override
    //TODO: enable use of contentQuery?
    public Iterator<Content> contentForTopic(final Long topicId, ContentQuery contentQuery) {
        return contentIterator(BRAND_SERIES_AND_ITEMS_TABLES, new ListingCursorBuilder() {

            @Override
            public DBCursor cursorFor(ContentCategory category) {
                return contentTables.collectionFor(category).find(where().fieldEquals("topics.topic", topicId).build()).sort(sort().ascending(ID).build());
            }
        });
    }
}
