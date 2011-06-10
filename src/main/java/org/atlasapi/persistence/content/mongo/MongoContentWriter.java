package org.atlasapi.persistence.content.mongo;

import static com.metabroadcast.common.persistence.mongo.MongoBuilders.where;
import static org.atlasapi.persistence.media.entity.DescriptionTranslator.CANONICAL_URI;

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
import org.atlasapi.persistence.media.entity.ContainerTranslator;
import org.atlasapi.persistence.media.entity.DescriptionTranslator;
import org.atlasapi.persistence.media.entity.ItemTranslator;
import org.joda.time.DateTime;

import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.persistence.mongo.MongoQueryBuilder;
import com.metabroadcast.common.time.Clock;
import com.mongodb.DBCollection;

public class MongoContentWriter implements ContentWriter {

    private final Clock clock;
    private final NewLookupWriter lookupStore;

    private final ItemTranslator itemTranslator = new ItemTranslator();
    private final ContainerTranslator containerTranslator = new ContainerTranslator();

    private final DBCollection children;
    private final DBCollection topLevelItems;
    private final DBCollection containers;
    private final DBCollection programmeGroups;

    public MongoContentWriter(DatabasedMongo mongo, NewLookupWriter lookupStore, Clock clock) {
        this.lookupStore = lookupStore;
        this.clock = clock;

        children = mongo.collection("children");
        topLevelItems = mongo.collection("topLevelItems");
        containers = mongo.collection("containers");
        programmeGroups = mongo.collection("programmeGroups");
    }

    @Override
    public void createOrUpdate(Item item) {
        updateFetchData(item);

        MongoQueryBuilder where = where().fieldEquals(DescriptionTranslator.CANONICAL_URI, item.getCanonicalUri());
        if (item.getContainer() != null) {
            
            if(item instanceof Episode) {
                includeEpisodeInSeriesAndBrand((Episode)item);
            } else {
                includeItemInTopLevelContainer(item);
            }
            
            children.update(where.build(), itemTranslator.toDB(item), true, false);
            
        } else {
            if (item instanceof Episode) {
                throw new IllegalArgumentException("Can't write episode with no container");
            }
            topLevelItems.update(where.build(), itemTranslator.toDB(item), true, false);
        }

        lookupStore.ensureLookup(item);
    }

    private void includeEpisodeInSeriesAndBrand(Episode episode) {
        
        if(episode.getSeriesRef() == null) { //just ensure item in container.
            includeItemInContainer(episode, containers);
            return;
        }

        
        MongoQueryBuilder brandWhere = where().fieldEquals(CANONICAL_URI, episode.getContainer().getUri());
        Maybe<Container<?>> maybeBrand = Maybe.firstElementOrNothing(containerTranslator.fromDBObjects(brandWhere.find(containers)));
        
        MongoQueryBuilder seriesWhere = where().fieldEquals(CANONICAL_URI, episode.getSeriesRef().getUri());
        Maybe<Container<?>> maybeSeries = Maybe.firstElementOrNothing(containerTranslator.fromDBObjects(seriesWhere.find(programmeGroups)));
        
        
        if(maybeBrand.isNothing() || maybeSeries.isNothing()) {
            throw new IllegalStateException(String.format("Container or series not found for %s",episode.getCanonicalUri()));
        }
        
        Container<?> container = maybeBrand.requireValue();
        container.setChildRefs(mergeChildRefs(ImmutableList.of(episode.childRef()), maybeBrand));
        containers.update(brandWhere.build(), containerTranslator.toDB(container), true, false);
        
        Container<?> series = maybeSeries.requireValue();
        series.setChildRefs(mergeChildRefs(ImmutableList.of(episode.childRef()), maybeSeries));
        programmeGroups.update(seriesWhere.build(), containerTranslator.toDB(series), true, false);
    }

    private void includeItemInTopLevelContainer(Item item) {
        includeItemInContainer(item, containers);
    }

    private void includeItemInContainer(Item item, DBCollection collection) {
        MongoQueryBuilder where = where().fieldEquals(DescriptionTranslator.CANONICAL_URI, item.getContainer().getUri());

        Maybe<Container<?>> oldContainer = Maybe.firstElementOrNothing(containerTranslator.fromDBObjects(where.find(collection)));
        if (oldContainer.hasValue()) {
            Container<?> container = oldContainer.requireValue();
            container.setChildRefs(mergeChildRefs(ImmutableList.of(item.childRef()), oldContainer));
            collection.update(where.build(), containerTranslator.toDB(container), true, false);
        } else {
            throw new IllegalStateException(String.format("Container %s not found in %s",item.getContainer().getUri(), collection.getName()));
        }
    }

    @Override
    public void createOrUpdate(Container<?> container) {

        if (container instanceof Series) {
            
            createOrUpdateContainer(container, programmeGroups);
            
            if(((Series) container).getParent() != null) {
                includeSeriesInBrand((Series)container);
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

    private void includeSeriesInBrand(Series series) {
        DBCollection collection = containers;

        MongoQueryBuilder where = where().fieldEquals(DescriptionTranslator.CANONICAL_URI, series.getParent().getUri());
        Maybe<Container<?>> oldContainer = Maybe.firstElementOrNothing(containerTranslator.fromDBObjects(where.find(collection)));
        if (oldContainer.hasValue()) {
            Brand container = (Brand) oldContainer.requireValue();
            container.setSeriesRefs(mergeChildRefs(ImmutableList.of(series.childRef()), oldContainer));
            collection.update(where.build(), containerTranslator.toDB(container), true, false);
        } else {
            throw new IllegalStateException(String.format("Brand %s not found for series %s",series.getParent().getUri(), series.getCanonicalUri()));
        }
        
    }

    private void createOrUpdateContainer(Container<?> container, DBCollection collection) {
        MongoQueryBuilder where = where().fieldEquals(DescriptionTranslator.CANONICAL_URI, container.getCanonicalUri());

        Maybe<Container<?>> oldContainer = Maybe.firstElementOrNothing(containerTranslator.fromDBObjects(where.find(collection)));
        container.setChildRefs(mergeChildRefs(container, oldContainer));

        container.setLastFetched(clock.now());
        setThisOrChildLastUpdated(container);

        collection.update(where.build(), containerTranslator.toDB(container), true, false);

        lookupStore.ensureLookup(container);
    }

    private List<ChildRef> mergeChildRefs(Container<?> container, Maybe<Container<?>> oldContainer) {
        return mergeChildRefs(container.getChildRefs(), oldContainer);
    }
    
    private List<ChildRef> mergeChildRefs(Iterable<ChildRef> currentChildRefs, Maybe<Container<?>> oldContainer) {
        ImmutableList.Builder<ChildRef> childRefs = ImmutableList.builder();
        childRefs.addAll(currentChildRefs);
        if (oldContainer.hasValue()) {
            childRefs.addAll(oldContainer.requireValue().getChildRefs());
        }
        return ChildRef.dedupeAndSort(childRefs.build());
    }

    private void updateFetchData(Item item) {
        item.setLastFetched(clock.now());
        setThisOrChildLastUpdated(item);
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

    private DateTime setThisOrChildLastUpdated(Container<?> playlist) {
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
