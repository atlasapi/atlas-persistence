package org.atlasapi.persistence.content.mongo;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.metabroadcast.common.persistence.mongo.MongoBuilders.where;
import static com.metabroadcast.common.persistence.mongo.MongoConstants.ID;
import static com.metabroadcast.common.persistence.mongo.MongoConstants.SINGLE;
import static com.metabroadcast.common.persistence.mongo.MongoConstants.UPSERT;

import java.util.Set;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.ChildRef;
import org.atlasapi.media.entity.Clip;
import org.atlasapi.media.content.Container;
import org.atlasapi.media.entity.Encoding;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Location;
import org.atlasapi.media.entity.Series;
import org.atlasapi.media.entity.Version;
import org.atlasapi.persistence.content.ContentCategory;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.lookup.NewLookupWriter;
import org.atlasapi.persistence.media.entity.ContainerTranslator;
import org.atlasapi.persistence.media.entity.IdentifiedTranslator;
import org.atlasapi.persistence.media.entity.ItemTranslator;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Collections2;
import com.google.common.collect.Sets;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.persistence.mongo.MongoConstants;
import com.metabroadcast.common.persistence.mongo.MongoQueryBuilder;
import com.metabroadcast.common.time.Clock;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class MongoContentWriter implements ContentWriter {

    private final Logger log = LoggerFactory.getLogger(MongoContentWriter.class);
    
    private final Clock clock;
    private final NewLookupWriter lookupStore;

    private final ItemTranslator itemTranslator;
    private final ContainerTranslator containerTranslator;

    private ChildRefWriter childRefWriter;

    private final DBCollection children;
    private final DBCollection topLevelItems;
    private final DBCollection containers;
    private final DBCollection programmeGroups;

    public MongoContentWriter(DatabasedMongo mongo, NewLookupWriter lookupStore, Clock clock) {
        this.lookupStore = lookupStore;
        this.clock = clock;

        MongoContentTables contentTables = new MongoContentTables(mongo);
        
        children = contentTables.collectionFor(ContentCategory.CHILD_ITEM);
        topLevelItems = contentTables.collectionFor(ContentCategory.TOP_LEVEL_ITEM);
        containers = contentTables.collectionFor(ContentCategory.CONTAINER);
        programmeGroups = contentTables.collectionFor(ContentCategory.PROGRAMME_GROUP);
        
        this.childRefWriter = new ChildRefWriter(mongo);
        NumberToShortStringCodec idCodec = new SubstitutionTableNumberCodec();
        this.itemTranslator = new ItemTranslator(idCodec);
        this.containerTranslator = new ContainerTranslator(idCodec);
    }

    @Override
    public void createOrUpdate(Item item) {
        checkNotNull(item, "Tried to persist null item");
        
        setThisOrChildLastUpdated(item);
        item.setLastFetched(clock.now());
        
        MongoQueryBuilder where = where().fieldEquals(IdentifiedTranslator.ID, item.getCanonicalUri());

        if (!item.hashChanged(itemTranslator.hashCodeOf(item))) {
            log.debug("Item {} hash not changed. Not writing.", item.getCanonicalUri());
        	return;
        } 
        
        log.debug("Item {} hash changed so writing to db", item.getCanonicalUri());
        
        DBObject itemDbo = itemTranslator.toDB(item);
        
		if (item instanceof Episode) {
            if (item.getContainer() == null) {
                throw new IllegalArgumentException(String.format("Episodes must have containers: Episode %s", item.getCanonicalUri()));
            }

            childRefWriter.includeEpisodeInSeriesAndBrand((Episode) item);
            children.update(where.build(), itemDbo, UPSERT, SINGLE);

            remove(item.getCanonicalUri(), topLevelItems);

        } else if (item.getContainer() != null) {

            childRefWriter.includeItemInTopLevelContainer(item);
            children.update(where.build(), itemDbo, UPSERT, SINGLE);

            remove(item.getCanonicalUri(), topLevelItems);
        } else {
            topLevelItems.update(where.build(), itemDbo, UPSERT, SINGLE);
            
            //disabled for now. need to remove the childref from the brand/series if enabled
            //remove(item.getCanonicalUri(), children);
        }

        lookupStore.ensureLookup(item);
    }

    private void remove(String canonicalUri, DBCollection containingCollection) {
        DBObject find = containingCollection.findOne(new BasicDBObject(MongoConstants.ID, canonicalUri), new BasicDBObject(ID, 1));
        if(find != null) {
            containingCollection.remove(new BasicDBObject(MongoConstants.ID, canonicalUri));
        }
    }

    @Override
    public void createOrUpdate(Container container) {
        checkNotNull(container);
        
        setThisOrChildLastUpdated(container);
        container.setLastFetched(clock.now());

        if (!container.hashChanged(containerTranslator.hashCodeOf(container))) {
            log.debug("Container {} hash not changed. Not writing.", container.getCanonicalUri());
            return;
        } 
        
        log.debug("Container {} hash changed so writing to db", container.getCanonicalUri());

        DBObject containerDbo = containerTranslator.toDB(container);

        if (container instanceof Series) {
            
            createOrUpdateContainer(container, programmeGroups, containerDbo);
            
            if(((Series) container).getParent() != null) {
                Series series = (Series)container;
                childRefWriter.includeSeriesInTopLevelContainer(series);
                //this isn't a top-level series so ensure it's not in the container table.
                containers.remove(where().idEquals(series.getCanonicalUri()).build());
                return;
            }
            
        }
        
        createOrUpdateContainer(container, containers, containerDbo);
        
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

    private void createOrUpdateContainer(Container container, DBCollection collection, DBObject containerDbo) {
        MongoQueryBuilder where = where().fieldEquals(IdentifiedTranslator.ID, container.getCanonicalUri());
        
        collection.update(where.build(), set(containerDbo), true, false);

        lookupStore.ensureLookup(container);
    }

    private BasicDBObject set(DBObject dbo) {
        dbo.removeField(MongoConstants.ID);
        BasicDBObject containerUpdate = new BasicDBObject(MongoConstants.SET, dbo);
        return containerUpdate;
    }
    
    private void setThisOrChildLastUpdated(Item item) {
        DateTime thisOrChildLastUpdated = thisOrChildLastUpdated(null, item.getLastUpdated());
        
        for (Clip clip : item.getClips()) {
            thisOrChildLastUpdated = thisOrChildLastUpdated(thisOrChildLastUpdated, clip.getLastUpdated());
        }

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
