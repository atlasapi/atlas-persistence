package org.atlasapi.persistence.content.mongo;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.metabroadcast.common.persistence.mongo.MongoBuilders.where;
import static org.atlasapi.persistence.content.ContentTable.CHILD_ITEMS;
import static org.atlasapi.persistence.content.ContentTable.PROGRAMME_GROUPS;
import static org.atlasapi.persistence.content.ContentTable.TOP_LEVEL_CONTAINERS;
import static org.atlasapi.persistence.content.ContentTable.TOP_LEVEL_ITEMS;
import static org.atlasapi.persistence.content.listing.ContentListingProgress.progressFor;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentTable;
import org.atlasapi.persistence.content.listing.ContentLister;
import org.atlasapi.persistence.content.listing.ContentListingCriteria;
import org.atlasapi.persistence.content.listing.ContentListingHandler;
import org.atlasapi.persistence.media.entity.ContainerTranslator;
import org.atlasapi.persistence.media.entity.ItemTranslator;
import org.joda.time.DateTime;

import com.google.common.base.Function;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Ordering;
import com.metabroadcast.common.persistence.mongo.MongoConstants;
import com.metabroadcast.common.persistence.mongo.MongoQueryBuilder;
import com.metabroadcast.common.persistence.mongo.MongoSortBuilder;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class MongoContentLister implements ContentLister, LastUpdatedContentFinder {

    private static final MongoSortBuilder SORT_BY_ID = new MongoSortBuilder().ascending(MongoConstants.ID); 
    
    private final ContainerTranslator containerTranslator = new ContainerTranslator();
    private final ItemTranslator itemTranslator = new ItemTranslator();

    private int batchSize = -100;

    private final MongoContentTables contentTables;

    public MongoContentLister(MongoContentTables contentTables) {
        this.contentTables = contentTables;
    }
    
    public MongoContentLister withBatchSize(int positiveBatchSize) {
        this.batchSize = positiveBatchSize;
        return this;
    }
    
    @Override
    public boolean listContent(Set<ContentTable> tables, ContentListingCriteria criteria, ContentListingHandler handler) {
        checkNotNull(handler, "Content Listing handler can't be null");
        
        if(criteria == null) {
            criteria = ContentListingCriteria.defaultCriteria();
        }
        
        Set<Publisher> publishers = criteria.getPublishers();
        publishers = publishers == null || publishers.isEmpty() ? ImmutableSet.copyOf(Publisher.values()) : publishers;
        
        String fromId = criteria.getProgress().getUri();
        ContentTable table = criteria.getProgress().getTable();
        
        List<ContentTable> sortedTables = Ordering.natural().reverse().immutableSortedCopy(tables); 
        List<ContentTable> remainTables = sortedTables.subList(table == null ? 0 : sortedTables.indexOf(table), sortedTables.size());
        
        int total = countRows(sortedTables, publishers);
        AtomicInteger progress = new AtomicInteger(criteria.getProgress().count());
        
        for (ContentTable contentTable : remainTables) {
            
            boolean shouldContinue = listContent(fromId, total, progress, publishers, handler, contentTable);
            if(shouldContinue) {
                fromId = null;
            } else {
                return false;
            }
            
        }
        return true;
    }
    
    private int countRows(List<ContentTable> sortedTables, Set<Publisher> publishers) {
        int total = 0;
        MongoQueryBuilder query = where();
        if(!publishers.equals(ImmutableSet.copyOf(Publisher.values()))) {
            query.fieldIn("publisher", Iterables.transform(publishers, Publisher.TO_KEY));
        }
        for (ContentTable contentTable : sortedTables) {
            total += contentTables.collectionFor(contentTable).find(query.build()).count();
        }
        return total;
    }

    private boolean listContent(String start, int total, AtomicInteger progress, Set<Publisher> publishers, ContentListingHandler handler, ContentTable table) {
        DBCollection collection = contentTables.collectionFor(table);
        while (true) {
            
            MongoQueryBuilder query = queryFor(start, publishers);
            
            List<Content> contents = ImmutableList.copyOf(Iterables.transform(query.find(collection, SORT_BY_ID, batchSize), TRANSLATORS.get(table)));
            
            if (Iterables.isEmpty(contents)) {
                return true;
            }
            
            for (Content content : contents) {
                if(!handler.handle(content, progressFor(content, table).withCount(progress.incrementAndGet()).withTotal(total))){
                    return false;
                }
            }
            Content last = Iterables.getLast(contents);
            start = last.getCanonicalUri();
        }
    }

    private MongoQueryBuilder queryFor(String start, Set<Publisher> publishers) {
        MongoQueryBuilder query = where().fieldIn("publisher", Iterables.transform(publishers, Publisher.TO_KEY));
        if (start != null) {
            query.fieldGreaterThan(MongoConstants.ID, start);
        }
        return query;
    }
    
    private static final List<ContentTable> BRAND_SERIES_AND_ITEMS_TABLES = ImmutableList.of(ContentTable.TOP_LEVEL_CONTAINERS, ContentTable.PROGRAMME_GROUPS, ContentTable.TOP_LEVEL_ITEMS, ContentTable.CHILD_ITEMS);
    
    @Override
    public Iterator<Content> updatedSince(final Publisher publisher, final DateTime when) {
        return new AbstractIterator<Content>() {

            private final Iterator<ContentTable> tablesIt = BRAND_SERIES_AND_ITEMS_TABLES.iterator();
            private Iterator<DBObject> currentResults = Iterators.emptyIterator();
            private Function<DBObject, ? extends Content> currentTranslator;
            
            @Override
            protected Content computeNext() {
                while (!currentResults.hasNext()) {
                    if (!tablesIt.hasNext()) {
                        return endOfData();
                    }
                    ContentTable table = tablesIt.next();
                    currentTranslator = TRANSLATORS.get(table);
                    DBObject query = where()
                        .fieldEquals("publisher", publisher.key())
                        .fieldAfter("thisOrChildLastUpdated", when)
                    .build();
                    currentResults = contentTables.collectionFor(table).find(query);
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

    private final ImmutableMap<ContentTable, Function<DBObject, ? extends Content>> TRANSLATORS = ImmutableMap.<ContentTable, Function<DBObject, ? extends Content>>of(
            CHILD_ITEMS, TO_ITEM, 
            PROGRAMME_GROUPS, TO_CONTAINER, 
            TOP_LEVEL_ITEMS, TO_ITEM, 
            TOP_LEVEL_CONTAINERS, TO_CONTAINER);

}
