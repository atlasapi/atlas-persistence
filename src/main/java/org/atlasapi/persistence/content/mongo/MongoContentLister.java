package org.atlasapi.persistence.content.mongo;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.metabroadcast.common.persistence.mongo.MongoBuilders.where;
import static org.atlasapi.persistence.content.ContentTable.CHILD_ITEMS;
import static org.atlasapi.persistence.content.ContentTable.PROGRAMME_GROUPS;
import static org.atlasapi.persistence.content.ContentTable.TOP_LEVEL_CONTAINERS;
import static org.atlasapi.persistence.content.ContentTable.TOP_LEVEL_ITEMS;

import java.util.List;
import java.util.Set;

import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Item;
import org.atlasapi.persistence.content.ContentListingHandler;
import org.atlasapi.persistence.content.ContentLister;
import org.atlasapi.persistence.content.ContentListingProgress;
import org.atlasapi.persistence.content.ContentTable;
import org.atlasapi.persistence.media.entity.ContainerTranslator;
import org.atlasapi.persistence.media.entity.ItemTranslator;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.metabroadcast.common.persistence.mongo.MongoConstants;
import com.metabroadcast.common.persistence.mongo.MongoQueryBuilder;
import com.metabroadcast.common.persistence.mongo.MongoSortBuilder;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class MongoContentLister implements ContentLister {

    private static final MongoSortBuilder SORT_BY_ID = new MongoSortBuilder().ascending(MongoConstants.ID); 
    
    private final ContainerTranslator containerTranslator = new ContainerTranslator();
    private final ItemTranslator itemTranslator = new ItemTranslator();

    private int batchSize = 10;

    private final MongoContentTables contentTables;

    public MongoContentLister(MongoContentTables contentTables) {
        this.contentTables = contentTables;
    }
    
    public MongoContentLister withBatchSize(int positiveBatchSize) {
        this.batchSize = positiveBatchSize;
        return this;
    }
    
    @Override
    public void listContent(Set<ContentTable> tables, ContentListingProgress progress, ContentListingHandler handler) {
        checkNotNull(handler, "Content Listing handler can't be null");
        
        String fromId = null;
        ContentTable table = null;
        
        if(progress != null) {
            fromId = progress.getUri();
            table = progress.getTable();
        }
        
        List<ContentTable> sortedTables = Ordering.natural().immutableSortedCopy(tables); 
        
        List<ContentTable> remainTables = sortedTables.subList(table == null ? 0 : sortedTables.indexOf(table), sortedTables.size());
        
        for (ContentTable contentTable : remainTables) {
            
            boolean shouldContinue = listContent(fromId, handler, contentTable, translatorFor(contentTable));
            if(shouldContinue) {
                fromId = null;
            } else {
                return;
            }
            
        }
        
    }
    
    private Function<DBObject, ? extends Content> translatorFor(ContentTable table) {
        return ImmutableMap.<ContentTable, Function<DBObject, ? extends Content>>of(
                CHILD_ITEMS, TO_ITEM, 
                PROGRAMME_GROUPS, TO_CONTAINER, 
                TOP_LEVEL_ITEMS, TO_ITEM, 
                TOP_LEVEL_CONTAINERS, TO_CONTAINER).get(table);
    }
    
    private boolean listContent(String start, ContentListingHandler handler, ContentTable table, Function<DBObject, ? extends Content> translatorFunction) {
        DBCollection collection = contentTables.collectionFor(table);
        while (true) {
            List<Content> contents = ImmutableList.copyOf(contentBatch(start, collection, translatorFunction));
            if (Iterables.isEmpty(contents)) {
                return true;
            }
            for (Content content : contents) {
                if(!handler.handle(content, ContentListingProgress.valueOf(content, table))){
                    return false;
                }
            }
            Content last = Iterables.getLast(contents);
            start = last.getCanonicalUri();
        }
    }

    private <T> Iterable<T> contentBatch(String fromId, DBCollection collection, Function<DBObject, ? extends T> translatorFunction) {
        return Iterables.transform(dboBatch(batchSize, fromId, collection), translatorFunction);
    }

    private Iterable<DBObject> dboBatch(int batchSize, String fromId, DBCollection collection) {
        MongoQueryBuilder query = where();
        if (fromId != null) {
            query.fieldGreaterThan(MongoConstants.ID, fromId);
        }
        Iterable<DBObject> dbos = query.find(collection, SORT_BY_ID, batchSize);
        return dbos;
    }
    
    private final Function<DBObject, Container<?>> TO_CONTAINER = new Function<DBObject, Container<?>>() {
        @Override
        public Container<?> apply(DBObject input) {
            return containerTranslator.fromDBObject(input, null);
        }
    };
    
    private final Function<DBObject, Item> TO_ITEM = new Function<DBObject, Item>() {
        @Override
        public Item apply(DBObject input) {
            return itemTranslator.fromDBObject(input, null);
        }
    };

}
