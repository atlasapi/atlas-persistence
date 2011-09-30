package org.atlasapi.persistence.content.mongo;

import static com.google.common.collect.ImmutableList.builder;
import static com.metabroadcast.common.persistence.mongo.MongoBuilders.where;
import static com.metabroadcast.common.persistence.mongo.MongoConstants.ID;
import static org.atlasapi.persistence.content.ContentCategory.CHILD_ITEM;
import static org.atlasapi.persistence.content.ContentCategory.CONTAINER;
import static org.atlasapi.persistence.content.ContentCategory.PROGRAMME_GROUP;
import static org.atlasapi.persistence.content.ContentCategory.TOP_LEVEL_ITEM;

import java.util.Iterator;
import java.util.List;

import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentCategory;
import org.atlasapi.persistence.content.listing.ContentLister;
import org.atlasapi.persistence.content.listing.ContentListingCriteria;
import org.atlasapi.persistence.media.entity.ContainerTranslator;
import org.atlasapi.persistence.media.entity.ItemTranslator;
import org.joda.time.DateTime;

import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.persistence.mongo.MongoConstants;
import com.metabroadcast.common.persistence.mongo.MongoQueryBuilder;
import com.metabroadcast.common.persistence.mongo.MongoSortBuilder;
import com.mongodb.DBObject;

public class MongoContentLister implements ContentLister, LastUpdatedContentFinder {

    private final ContainerTranslator containerTranslator = new ContainerTranslator();
    private final ItemTranslator itemTranslator = new ItemTranslator();

    private final MongoContentTables contentTables;

    public MongoContentLister(DatabasedMongo mongo) {
        this.contentTables = new MongoContentTables(mongo);
    }
    
    @Override
    public Iterator<Content> listContent(ContentListingCriteria criteria) {
        List<Publisher> publishers = remainingPublishers(criteria);
        List<ContentCategory> tables = remainingTables(criteria);
        
        if(publishers.isEmpty()) {
            return Iterators.emptyIterator();
        }
        if(publishers.size() == 1) {
            return contentIterator(tables, publishers.get(0), criteria);
        }
        
        Builder<Iterator<Content>> iteratorBuilder = builder();
        iteratorBuilder.add(contentIterator(tables, publishers.get(0), criteria)).addAll(iteratorsFor(publishers.subList(1, publishers.size()), criteria));
        return Iterators.concat(iteratorBuilder.build().iterator());
    }
    

    private Iterable<Iterator<Content>> iteratorsFor(List<Publisher> publishers, final ContentListingCriteria criteria) {
        return Iterables.transform(publishers, new Function<Publisher, Iterator<Content>>() {
            @Override
            public Iterator<Content> apply(Publisher publisher) {
                return contentIterator(criteria.getCategories(), publisher, null, null);
            }
        });
    }

    private Iterator<Content> contentIterator(List<ContentCategory> tables, Publisher publisher, ContentListingCriteria criteria) {
        return contentIterator(tables, publisher, criteria.getProgress().getUri(), null);
    }

    private static final List<ContentCategory> BRAND_SERIES_AND_ITEMS_TABLES = ImmutableList.of(CONTAINER, PROGRAMME_GROUP, TOP_LEVEL_ITEM, CHILD_ITEM);
    
    @Override
    public Iterator<Content> updatedSince(final Publisher publisher, final DateTime when) {
        return contentIterator(BRAND_SERIES_AND_ITEMS_TABLES, publisher, null, when);
    }

    private Iterator<Content> contentIterator(final List<ContentCategory> tables, final Publisher publisher, final String id, final DateTime when) {
        return new AbstractIterator<Content>() {

            private final Iterator<ContentCategory> tablesIt = tables.iterator();
            private Iterator<DBObject> currentResults = Iterators.emptyIterator();
            private Function<DBObject, ? extends Content> currentTranslator;
            private String uri = id;
            
            @Override
            protected Content computeNext() {
                while (!currentResults.hasNext()) {
                    if (!tablesIt.hasNext()) {
                        return endOfData();
                    }
                    ContentCategory table = tablesIt.next();
                    currentTranslator = TRANSLATORS.get(table);
                    currentResults = contentTables.collectionFor(table).find(queryFor(uri, when, publisher)).sort(sortFor(uri, when));
                    uri = null;//only use the id for the first table.
                }
                return currentTranslator.apply(currentResults.next());
            }
        };
    }
    
    private DBObject sortFor(String uri, DateTime when) {
        MongoSortBuilder sort = new MongoSortBuilder().ascending("publisher");
        if(when != null) {
            sort.ascending("thisOrChildLastUpdated");
        }
        if(!Strings.isNullOrEmpty(uri)) {
            sort.ascending(MongoConstants.ID).build(); 
        }
        return sort.build();
    }

    private DBObject queryFor(final String uri, final DateTime when, Publisher publisher) {
        MongoQueryBuilder query = where().fieldEquals("publisher", publisher.key());
        if (!Strings.isNullOrEmpty(uri)) {
            query.fieldGreaterThan(ID, uri);
        }
        if(when != null) {
            query.fieldAfter("thisOrChildLastUpdated", when);
        }
        return query.build();
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

}
