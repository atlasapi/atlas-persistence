package org.atlasapi.media.content;

import static com.google.common.base.Predicates.equalTo;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.Iterables.filter;
import static com.metabroadcast.common.persistence.mongo.MongoBuilders.where;
import static org.atlasapi.persistence.media.entity.ContainerTranslator.CHILDREN_KEY;

import java.util.List;

import org.atlasapi.media.content.Brand;
import org.atlasapi.media.content.ChildRef;
import org.atlasapi.media.content.Container;
import org.atlasapi.media.content.Episode;
import org.atlasapi.media.content.Item;
import org.atlasapi.media.content.Series;
import org.atlasapi.media.content.util.ContentCategory;
import org.atlasapi.persistence.media.entity.ContainerTranslator;
import org.joda.time.DateTime;

import com.google.common.collect.ImmutableList;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class ChildRefWriter {

    private final DBCollection containers;
    private final DBCollection programmeGroups;
    
    private final ContainerTranslator containerTranslator;

    public ChildRefWriter(DatabasedMongo mongo) {
        MongoContentTables mongoTables = new MongoContentTables(mongo);
        this.containers = mongoTables.collectionFor(ContentCategory.CONTAINER);
        this.programmeGroups = mongoTables.collectionFor(ContentCategory.PROGRAMME_GROUP);
        this.containerTranslator = new ContainerTranslator(new SubstitutionTableNumberCodec());
    }

    public void includeEpisodeInSeriesAndBrand(Episode episode) {

        ChildRef childRef = episode.childRef();

        if (episode.getSeriesRef() == null) { // just ensure item in container.
            includeChildRefInContainer(episode.getContainer().getUri(), childRef, containers, CHILDREN_KEY);
            return;
        }

        // otherwise retrieve the child references for both series and brand, if
        // either are missing, change nothing and error out.
        String brandUri = episode.getContainer().getUri();
        String seriesUri = episode.getSeriesRef().getUri();

        Maybe<Container> maybeBrand = getContainer(brandUri, containers);
        Maybe<Container> maybeSeries = getContainer(seriesUri, programmeGroups);

        if (maybeBrand.isNothing() || maybeSeries.isNothing()) {
            throw new IllegalStateException(String.format("Container or series not found for episode %s", episode.getCanonicalUri()));
        }

        addChildRef(childRef, containers, maybeBrand.requireValue());
        addChildRef(childRef, programmeGroups, maybeSeries.requireValue());

    }

    public void includeSeriesInTopLevelContainer(Series series) {
        String containerUri = series.getParent().getUri();
        
        Maybe<Container> maybeContainer = getContainer(containerUri, containers);

        if (maybeContainer.hasValue()) {
            Container container = maybeContainer.requireValue();
            if(container instanceof Brand) {
                Brand brand = (Brand) container;
                List<ChildRef> merged = mergeChildRefs(series.childRef(), brand.getSeriesRefs());
                brand.setSeriesRefs(merged);
                brand.setThisOrChildLastUpdated(laterOf(brand.getThisOrChildLastUpdated(), series.getThisOrChildLastUpdated()));
                containers.save(containerTranslator.toDBO(container, true));
            } else {
                throw new IllegalStateException(String.format("Container %s for series child ref %s is not brand", containerUri, series.getCanonicalUri()));
            }
        } else {
            throw new IllegalStateException(String.format("Container %s not found in %s for series child ref %s", containerUri, containers.getName(), series.getCanonicalUri()));
        }
    }

    private DateTime laterOf(DateTime left, DateTime right) {
        if (left == null) {
            return right;
        }
        if (right == null) {
            return left;
        }
        return left.isAfter(right) ? left : right;
    }

    public void includeItemInTopLevelContainer(Item item) {
        includeChildRefInContainer(item.getContainer().getUri(), item.childRef(), containers, CHILDREN_KEY);
    }

    private void includeChildRefInContainer(String containerUri, ChildRef ref, DBCollection collection, String key) {

        Maybe<Container> maybeContainer = getContainer(containerUri, collection);

        if (maybeContainer.hasValue()) {
            addChildRef(ref, collection, maybeContainer.requireValue());
        } else {
            throw new IllegalStateException(String.format("Container %s not found in %s for child ref %s", containerUri, collection.getName(), ref.getUri()));
        }
    }

    private void addChildRef(ChildRef ref, DBCollection collection, Container container) {
        List<ChildRef> merged = mergeChildRefs(ref, container.getChildRefs());
        container.setThisOrChildLastUpdated(laterOf(container.getThisOrChildLastUpdated(), ref.getUpdated()));
        container.setChildRefs(merged);
        collection.save(containerTranslator.toDBO(container, true));
    }

    private List<ChildRef> mergeChildRefs(ChildRef newChildRef, Iterable<ChildRef> currentChildRefs) {
        // filter out new refs so they can  be overwritten.
        currentChildRefs = filter(currentChildRefs, not(equalTo(newChildRef)));
        return ChildRef.dedupeAndSort(ImmutableList.<ChildRef> builder().addAll(currentChildRefs).add(newChildRef).build());
    }

    private Maybe<Container> getContainer(String canonicalUri, DBCollection collection) {
        DBObject dbo = collection.findOne(where().idEquals(canonicalUri).build());
        if (dbo == null) {
            return Maybe.nothing();
        }
        return Maybe.<Container> fromPossibleNullValue(containerTranslator.fromDB(dbo));
    }
    
}
