package org.atlasapi.persistence.content.mongo;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.metabroadcast.common.persistence.mongo.MongoBuilders.select;
import static com.metabroadcast.common.persistence.mongo.MongoBuilders.update;
import static com.metabroadcast.common.persistence.mongo.MongoBuilders.where;
import static org.atlasapi.persistence.content.ContentTable.CHILD_ITEMS;
import static org.atlasapi.persistence.content.ContentTable.PROGRAMME_GROUPS;
import static org.atlasapi.persistence.content.ContentTable.TOP_LEVEL_CONTAINERS;
import static org.atlasapi.persistence.content.ContentTable.TOP_LEVEL_ITEMS;
import static org.atlasapi.persistence.media.entity.ContainerTranslator.CHILDREN_KEY;
import static org.atlasapi.persistence.media.entity.ContainerTranslator.FULL_SERIES_KEY;

import java.util.List;
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
import org.atlasapi.persistence.media.entity.ChildRefTranslator;
import org.atlasapi.persistence.media.entity.ContainerTranslator;
import org.atlasapi.persistence.media.entity.DescribedTranslator;
import org.atlasapi.persistence.media.entity.DescriptionTranslator;
import org.atlasapi.persistence.media.entity.ItemTranslator;
import org.joda.time.DateTime;

import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.metabroadcast.common.base.Maybe;
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
    private final ChildRefTranslator childRefTranslator = new ChildRefTranslator();

    private final DBCollection children;
    private final DBCollection topLevelItems;
    private final DBCollection containers;
    private final DBCollection programmeGroups;

    public MongoContentWriter(MongoContentTables contentTables, NewLookupWriter lookupStore, Clock clock) {
        this.lookupStore = lookupStore;
        this.clock = clock;

        children = contentTables.collectionFor(CHILD_ITEMS);
        topLevelItems = contentTables.collectionFor(TOP_LEVEL_ITEMS);
        containers = contentTables.collectionFor(TOP_LEVEL_CONTAINERS);
        programmeGroups = contentTables.collectionFor(PROGRAMME_GROUPS);
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
            
            includeEpisodeInSeriesAndBrand((Episode) item);
            children.update(where.build(), itemDbo, true, false);
            
        } else if(item.getContainer() != null) {
            
            includeItemInTopLevelContainer(item);
            children.update(where.build(), itemDbo, true, false);
            
        } else {
            topLevelItems.update(where.build(), itemDbo, true, false);
        }

        lookupStore.ensureLookup(item);
    }

    private void includeEpisodeInSeriesAndBrand(Episode episode) {
        
        if(episode.getSeriesRef() == null) { //just ensure item in container.
            includeChildRefInContainer(episode.getContainer().getUri(), episode.childRef(), containers, CHILDREN_KEY);
            return;
        }
        
        // otherwise retrieve the child references for both series and brand, if either are missing, change nothing and error out.
        String brandUri = episode.getContainer().getUri();
        String seriesUri = episode.getSeriesRef().getUri();
        
        Maybe<List<ChildRef>> maybeBrandChildren = getChildRefs(containers, brandUri, CHILDREN_KEY);
        Maybe<List<ChildRef>> maybeSeriesChildren = getChildRefs(programmeGroups, seriesUri, CHILDREN_KEY);
        
        if(maybeBrandChildren.isNothing() || maybeSeriesChildren.isNothing()) {
            throw new IllegalStateException(String.format("Container or series not found for episode %s",episode.getCanonicalUri()));
        }
        
        List<ChildRef> brandChildren = maybeBrandChildren.requireValue();
        brandChildren = mergeChildRefs(ImmutableList.of(episode.childRef()), brandChildren);
        containers.update(where().idEquals(brandUri).build(), update().setField(CHILDREN_KEY, childRefTranslator.toDBList(brandChildren)).build(), true, false);
        
        List<ChildRef> seriesChildren = maybeSeriesChildren.requireValue();
        seriesChildren = mergeChildRefs(ImmutableList.of(episode.childRef()), seriesChildren);
        programmeGroups.update(where().idEquals(seriesUri).build(),  update().setField(CHILDREN_KEY, childRefTranslator.toDBList(seriesChildren)).build(), true, false);
    }

    private Maybe<List<ChildRef>> getChildRefs(DBCollection collection, String containerUri, String key) {
        DBObject dbo = collection.findOne(where().idEquals(containerUri).build(), select().fields(key, DescribedTranslator.TYPE_KEY).build());
        
        if(dbo == null) {
            return Maybe.nothing();
        }
        
        return Maybe.<List<ChildRef>>fromPossibleNullValue(containerTranslator.fromDBObject(dbo, null).getChildRefs());
    }

    private void includeItemInTopLevelContainer(Item item) {
        includeChildRefInContainer(item.getContainer().getUri(), item.childRef(), containers, CHILDREN_KEY);
    }

    private void includeChildRefInContainer(String containerUri, ChildRef ref, DBCollection collection, String key) {
        
        Maybe<List<ChildRef>> currentChildRefs = getChildRefs(containers, containerUri, key);
        if (currentChildRefs.hasValue()) {
            List<ChildRef> containerChildRefs = currentChildRefs.requireValue();
            containerChildRefs = mergeChildRefs(ImmutableList.of(ref), containerChildRefs);
            collection.update(where().idEquals(containerUri).build(), update().setField(key, childRefTranslator.toDBList(containerChildRefs)).build(), true, false);
        } else {
            throw new IllegalStateException(String.format("Container %s not found in %s for child ref %s",containerUri, collection.getName(), ref.getUri()));
        }
    }

    @Override
    public void createOrUpdate(Container container) {
        checkNotNull(container);

        if (container instanceof Series) {
            
            createOrUpdateContainer(container, programmeGroups);
            
            if(((Series) container).getParent() != null) {
                Series series = (Series)container;
                includeChildRefInContainer(series.getParent().getUri(), series.childRef(), containers, FULL_SERIES_KEY);
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
    
    private List<ChildRef> mergeChildRefs(Iterable<ChildRef> newChildRefs, Iterable<ChildRef> currentChildRefs) {
        return ChildRef.dedupeAndSort(ImmutableList.<ChildRef>builder().addAll(currentChildRefs).addAll(newChildRefs).build());
    }

    private DateTime setThisOrChildLastUpdated(Item item) {
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

        return thisOrChildLastUpdated;
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
