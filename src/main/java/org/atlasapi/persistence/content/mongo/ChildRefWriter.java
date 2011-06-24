package org.atlasapi.persistence.content.mongo;

import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.ImmutableSet.copyOf;
import static com.google.common.collect.Iterables.filter;
import static com.metabroadcast.common.persistence.mongo.MongoBuilders.select;
import static com.metabroadcast.common.persistence.mongo.MongoBuilders.update;
import static com.metabroadcast.common.persistence.mongo.MongoBuilders.where;
import static org.atlasapi.persistence.media.entity.ContainerTranslator.CHILDREN_KEY;
import static org.atlasapi.persistence.media.entity.ContainerTranslator.FULL_SERIES_KEY;

import java.util.List;

import org.atlasapi.media.entity.ChildRef;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Series;
import org.atlasapi.persistence.content.ContentTable;
import org.atlasapi.persistence.media.entity.ChildRefTranslator;
import org.atlasapi.persistence.media.entity.ContainerTranslator;
import org.atlasapi.persistence.media.entity.DescribedTranslator;

import com.google.common.collect.ImmutableList;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class ChildRefWriter {

    private final DBCollection containers;
    private final DBCollection programmeGroups;

    private final ChildRefTranslator childRefTranslator = new ChildRefTranslator();
    private final ContainerTranslator containerTranslator = new ContainerTranslator();

    public ChildRefWriter(DatabasedMongo mongo) {
        MongoContentTables mongoTables = new MongoContentTables(mongo);
        containers = mongoTables.collectionFor(ContentTable.TOP_LEVEL_CONTAINERS);
        programmeGroups = mongoTables.collectionFor(ContentTable.PROGRAMME_GROUPS);
    }
    
    public void includeEpisodeInSeriesAndBrand(Episode episode) {
        
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
    
    public void includeSeriesInTopLevelContainer(Series series) {
        includeChildRefInContainer(series.getParent().getUri(), series.childRef(), containers, FULL_SERIES_KEY);
    }
    
    public void includeItemInTopLevelContainer(Item item) {
        includeChildRefInContainer(item.getContainer().getUri(), item.childRef(), containers, CHILDREN_KEY);
    }

    private void includeChildRefInContainer(String containerUri, ChildRef ref, DBCollection collection, String key) {
        
        Maybe<List<ChildRef>> currentChildRefs = getChildRefs(containers, containerUri, key);
        if (currentChildRefs.hasValue()) {
            List<ChildRef> containerChildRefs = currentChildRefs.requireValue();
            containerChildRefs = mergeChildRefs(ImmutableList.of(ref), containerChildRefs);
            collection.update(where().idEquals(containerUri).build(), update().setField(key, childRefTranslator.toDBList(containerChildRefs)).build(), false, false);
        } else {
            throw new IllegalStateException(String.format("Container %s not found in %s for child ref %s",containerUri, collection.getName(), ref.getUri()));
        }
    }
    
    private List<ChildRef> mergeChildRefs(Iterable<ChildRef> newChildRefs, Iterable<ChildRef> currentChildRefs) {
        currentChildRefs = filter(currentChildRefs, not(in(copyOf(newChildRefs)))); //filter out new refs so they can be overwritten.
        return ChildRef.dedupeAndSort(ImmutableList.<ChildRef>builder().addAll(currentChildRefs).addAll(newChildRefs).build());
    }
    
    private Maybe<List<ChildRef>> getChildRefs(DBCollection collection, String containerUri, String key) {
        DBObject dbo = collection.findOne(where().idEquals(containerUri).build(), select().fields(key, DescribedTranslator.TYPE_KEY).build());
        
        if(dbo == null) {
            return Maybe.nothing();
        }
        
        return Maybe.<List<ChildRef>>fromPossibleNullValue(containerTranslator.fromDBObject(dbo, null).getChildRefs());
    }
    
}
