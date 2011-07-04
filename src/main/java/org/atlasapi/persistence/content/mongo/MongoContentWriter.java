package org.atlasapi.persistence.content.mongo;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.metabroadcast.common.persistence.mongo.MongoBuilders.where;
import static org.atlasapi.persistence.content.ContentTable.CHILD_ITEMS;
import static org.atlasapi.persistence.content.ContentTable.PROGRAMME_GROUPS;
import static org.atlasapi.persistence.content.ContentTable.TOP_LEVEL_CONTAINERS;
import static org.atlasapi.persistence.content.ContentTable.TOP_LEVEL_ITEMS;

import java.util.Set;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.ChildRef;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Encoding;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Location;
import org.atlasapi.media.entity.Series;
import org.atlasapi.media.entity.Version;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.lookup.NewLookupWriter;
import org.atlasapi.persistence.media.entity.ContainerTranslator;
import org.atlasapi.persistence.media.entity.DescribedTranslator;
import org.atlasapi.persistence.media.entity.DescriptionTranslator;
import org.atlasapi.persistence.media.entity.ItemTranslator;
import org.joda.time.DateTime;

import com.google.common.collect.Collections2;
import com.google.common.collect.Sets;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.persistence.mongo.MongoConstants;
import com.metabroadcast.common.persistence.mongo.MongoQueryBuilder;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.metabroadcast.common.time.Clock;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class MongoContentWriter implements ContentWriter {

    private final Clock clock;
    private final NewLookupWriter lookupStore;

    private final ItemTranslator itemTranslator = new ItemTranslator();
    private final ContainerTranslator containerTranslator = new ContainerTranslator();

    private ChildRefWriter childRefWriter;

    private final DBCollection children;
    private final DBCollection topLevelItems;
    private final DBCollection containers;
    private final DBCollection programmeGroups;

    public MongoContentWriter(DatabasedMongo mongo, NewLookupWriter lookupStore, Clock clock) {
        this.lookupStore = lookupStore;
        this.clock = clock;

        MongoContentTables contentTables = new MongoContentTables(mongo);
        
        children = contentTables.collectionFor(CHILD_ITEMS);
        topLevelItems = contentTables.collectionFor(TOP_LEVEL_ITEMS);
        containers = contentTables.collectionFor(TOP_LEVEL_CONTAINERS);
        programmeGroups = contentTables.collectionFor(PROGRAMME_GROUPS);
        
        this.childRefWriter = new ChildRefWriter(mongo);
    }

    @Override
    public void createOrUpdate(Item item) {
        checkNotNull(item, "Tried to persist null item");
        
        setThisOrChildLastUpdated(item);
        
        MongoQueryBuilder where = where().fieldEquals(DescriptionTranslator.CANONICAL_URI, item.getCanonicalUri());
            
        DBObject itemDbo = itemTranslator.toDB(item);
        itemDbo.removeField(DescribedTranslator.LAST_FETCHED_KEY);
        
        if (!item.hashChanged(String.valueOf(itemDbo.hashCode()))) {
        	return;
        } 
        
        TranslatorUtils.fromDateTime(itemDbo, DescribedTranslator.LAST_FETCHED_KEY, clock.now());
        
		if (item instanceof Episode) {
            if (item.getContainer() == null) {
                throw new IllegalArgumentException("Episodes must have containers");
            } 
            
            childRefWriter.includeEpisodeInSeriesAndBrand((Episode) item);
            children.update(where.build(), itemDbo, true, false);
            
        } else if(item.getContainer() != null) {
            
            childRefWriter.includeItemInTopLevelContainer(item);
            children.update(where.build(), itemDbo, true, false);
            
        } else {
            topLevelItems.update(where.build(), itemDbo, true, false);
        }

        lookupStore.ensureLookup(item);
    }

    @Override
    public void createOrUpdate(Container container) {
        checkNotNull(container);

        if (container instanceof Series) {
            
            createOrUpdateContainer(container, programmeGroups);
            
            if(((Series) container).getParent() != null) {
                Series series = (Series)container;
                childRefWriter.includeSeriesInTopLevelContainer(series);
                return;
            }
            
        }
        
        createOrUpdateContainer(container, containers);
        
        // The series inside a brand cannot be top level items any more so we
        // remove them as outer elements
        if (container instanceof Brand) {
            Brand brand = (Brand) container;
            
            Set<String> urisToRemove = Sets.newHashSet(Collections2.transform(brand.getSeriesRefs(), ChildRef.TO_URI));
            if (!urisToRemove.isEmpty()) {
                containers.remove(where().idIn(urisToRemove).build());
            }
        }

    }

    private void createOrUpdateContainer(Container container, DBCollection collection) {
        MongoQueryBuilder where = where().fieldEquals(DescriptionTranslator.CANONICAL_URI, container.getCanonicalUri());
        
        container.setLastFetched(clock.now());
        setThisOrChildLastUpdated(container);

        collection.update(where.build(), set(containerTranslator.toDB(container)), true, false);

        lookupStore.ensureLookup(container);
    }

    private BasicDBObject set(DBObject dbo) {
        dbo.removeField(MongoConstants.ID);
        BasicDBObject containerUpdate = new BasicDBObject(MongoConstants.SET, dbo);
        return containerUpdate;
    }
    
    private void setThisOrChildLastUpdated(Item item) {
        DateTime thisOrChildLastUpdated = thisOrChildLastUpdated(null, item.getLastUpdated());

        for (Version version : item.getVersions()) {
            thisOrChildLastUpdated = thisOrChildLastUpdated(thisOrChildLastUpdated, version.getLastUpdated());

            for (Broadcast broadcast : version.getBroadcasts()) {
                thisOrChildLastUpdated = thisOrChildLastUpdated(thisOrChildLastUpdated, broadcast.getLastUpdated());
            }

            for (Encoding encoding : version.getManifestedAs()) {
                thisOrChildLastUpdated = thisOrChildLastUpdated(thisOrChildLastUpdated, encoding.getLastUpdated());

                for (Location location : encoding.getAvailableAt()) {
                    thisOrChildLastUpdated = thisOrChildLastUpdated(thisOrChildLastUpdated, location.getLastUpdated());
                }
            }
        }
        item.setThisOrChildLastUpdated(thisOrChildLastUpdated);
    }

    private DateTime setThisOrChildLastUpdated(Container playlist) {
        DateTime thisOrChildLastUpdated = thisOrChildLastUpdated(null, playlist.getLastUpdated());
        for (ChildRef item : playlist.getChildRefs()) {
            DateTime itemOrChildUpdated = item.getUpdated();
            thisOrChildLastUpdated = thisOrChildLastUpdated(thisOrChildLastUpdated, itemOrChildUpdated);
        }
        playlist.setThisOrChildLastUpdated(thisOrChildLastUpdated);
        return thisOrChildLastUpdated;
    }

    private DateTime thisOrChildLastUpdated(DateTime current, DateTime candidate) {
        if (candidate != null && (current == null || candidate.isAfter(current))) {
            return candidate;
        }
        return current;
    }
}
