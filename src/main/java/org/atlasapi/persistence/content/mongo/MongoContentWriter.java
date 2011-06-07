package org.atlasapi.persistence.content.mongo;

import static com.metabroadcast.common.persistence.mongo.MongoBuilders.where;

import java.util.List;
import java.util.Set;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.ChildRef;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.ContentGroup;
import org.atlasapi.media.entity.Encoding;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Identified;
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

import com.google.common.annotations.VisibleForTesting;
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

    public MongoContentWriter(DatabasedMongo mongo, NewLookupWriter lookupStore, Clock clock) {
        this.lookupStore = lookupStore;
        this.clock = clock;

        children = mongo.collection("children");
        topLevelItems = mongo.collection("topLevelItems");
        containers = mongo.collection("containers");
    }

    @Override
    public void createOrUpdate(Item item) {
        updateFetchData(item);

        MongoQueryBuilder where = where().fieldEquals(DescriptionTranslator.CANONICAL_URI, item.getCanonicalUri());
        if (item.getContainer() != null) {
            children.update(where.build(), itemTranslator.toDB(item), true, false);
            includeItemInContainer(item);
        } else {
            topLevelItems.update(where.build(), itemTranslator.toDB(item), true, false);
        }

        lookupStore.ensureLookup(item);
    }

    @VisibleForTesting
    protected void includeItemInContainer(Item item) {
        MongoQueryBuilder where = where().fieldEquals(DescriptionTranslator.CANONICAL_URI, item.getContainer().getCanonicalUri());

        Maybe<Container<?>> oldContainer = Maybe.firstElementOrNothing(containerTranslator.fromDBObjects(where.find(containers)));
        if (oldContainer.hasValue()) {
            Container<?> container = oldContainer.requireValue();
            container.setChildRefs(mergeChildRefs(ImmutableList.of(item.childRef()), oldContainer));
            containers.update(where.build(), containerTranslator.toDB(container), true, false);
        }
    }

    @Override
    public void createOrUpdate(Container<?> container) {
        MongoQueryBuilder where = where().fieldEquals(DescriptionTranslator.CANONICAL_URI, container.getCanonicalUri());

        Maybe<Container<?>> oldContainer = Maybe.firstElementOrNothing(containerTranslator.fromDBObjects(where.find(containers)));
        container.setChildRefs(mergeChildRefs(container, oldContainer));

        container.setLastFetched(clock.now());
        setThisOrChildLastUpdated(container);

        containers.update(where.build(), containerTranslator.toDB(container), true, false);

        // The series inside a brand cannot be top level items any more so we
        // remove them as outer elements
        if (container instanceof Brand) {
            Brand brand = (Brand) container;
            
            Set<String> urisToRemove = Sets.newHashSet(Collections2.transform(brand.getSeries(), Identified.TO_URI));
            if (!urisToRemove.isEmpty()) {
                containers.remove(where().idIn(urisToRemove).build());
            }
        }

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

    @Override
    public void createOrUpdateSkeleton(ContentGroup playlist) {
        // @deprecated. Replace with something better.
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
        for (Item item : playlist.getContents()) {
            DateTime itemOrChildUpdated = setThisOrChildLastUpdated(item);
            thisOrChildLastUpdated = thisOrChildLastUpdated(thisOrChildLastUpdated, itemOrChildUpdated);
            if (item instanceof Episode) {
                Series series = ((Episode) item).getSeries();
                if (series != null) {
                    series.setThisOrChildLastUpdated(thisOrChildLastUpdated(itemOrChildUpdated, series.getThisOrChildLastUpdated()));
                }
            }
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
