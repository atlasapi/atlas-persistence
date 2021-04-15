package org.atlasapi.persistence.content.mongo;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.EntityType;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.Series;
import org.atlasapi.persistence.content.ContentCategory;
import org.atlasapi.persistence.content.KnownTypeContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.atlasapi.persistence.media.entity.ContainerTranslator;
import org.atlasapi.persistence.media.entity.DescribedTranslator;
import org.atlasapi.persistence.media.entity.ItemTranslator;

import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.metabroadcast.common.persistence.mongo.MongoBuilders.where;

public class MongoContentResolver implements KnownTypeContentResolver {

    private final Logger log = LoggerFactory.getLogger(getClass());
    
    private final ItemTranslator itemTranslator;
    private final ContainerTranslator containerTranslator;
    private final MongoContentTables contentTables;
    private final LookupEntryStore lookupEntryStore;

    public MongoContentResolver(DatabasedMongo mongo, LookupEntryStore lookupEntryStore) {
        this.contentTables = new MongoContentTables(mongo);
        SubstitutionTableNumberCodec idCodec = new SubstitutionTableNumberCodec();
        this.containerTranslator = new ContainerTranslator(idCodec);
        this.itemTranslator = new ItemTranslator(idCodec);
        this.lookupEntryStore = checkNotNull(lookupEntryStore);
    }

    public ResolvedContent findByLookupRefs(Iterable<LookupRef> lookupRefs) {
        Builder<String, Identified> results = ImmutableMap.builder();
        Set<String> foundUris = Sets.newHashSet();
        Multimap<DBCollection, String> idsGroupedByTable = HashMultimap.create();
        for (LookupRef lookupRef : lookupRefs) {
            idsGroupedByTable.put(contentTables.collectionFor(lookupRef.category()), lookupRef.uri());
        }

        for (Entry<DBCollection, Collection<String>> lookupInOneTable : idsGroupedByTable.asMap().entrySet()) {

            DBCursor found = lookupInOneTable.getKey().find(where().idIn(lookupInOneTable.getValue()).build());
            if (found != null) {
                for (DBObject dbo : found) {
                    Identified model = toModel(dbo);
                    if (!foundUris.contains(model.getCanonicalUri())) {
                        results.put(model.getCanonicalUri(), model);
                        foundUris.add(model.getCanonicalUri());
                    }
                }
            }
        }

        ImmutableMap<String, Identified> res = results.build();

        addIdsToResults(res);
        return ResolvedContent.builder().putAll(res).build();
    }

    public ResolvedContent findChildrenForParent(Brand brand) {
        return findChildrenForParent(brand.getCanonicalUri(), true);
    }

    // This looks for children referencing this as a series. If you are a top level series and
    // you are looking to find children referencing you as a container instead of a series, use
    // the uri/boolean method. If you are looking for both make 2 calls and combine the results,
    // but beware of duplicates (and maybe this method should be doing that, but this it thrown
    // together in a bit of a hurry to fix equivalence running on all children).
    public ResolvedContent findChildrenForParent(Series series) {
        return findChildrenForParent(series.getCanonicalUri(),false);
    }

    public ResolvedContent findChildrenForParent(String parentUri, boolean isContainer) {
        String fieldName = isContainer ? ItemTranslator.CONTAINER : ItemTranslator.SERIES;
        Builder<String, Identified> results = ImmutableMap.builder();

        DBCollection childrenCollection = contentTables.collectionFor(ContentCategory.CHILD_ITEM);

        DBCursor found = childrenCollection.find(where().fieldEquals(fieldName, parentUri).build());
        if (found != null) {
            for (DBObject dbo : found) {
                Identified model = toModel(dbo);
                results.put(model.getCanonicalUri(), model);
            }
        }

        ImmutableMap<String, Identified> res = results.build();
        addIdsToResults(res);
        return ResolvedContent.builder().putAll(res).build();
    }

    public ResolvedContent findSeriesForBrand(Brand brand) {
        return findSeriesForBrand(brand.getCanonicalUri());
    }
    public ResolvedContent findSeriesForBrand(String containerUri) {
        Builder<String, Identified> results = ImmutableMap.builder();
        DBCollection seriesCollection = contentTables.collectionFor(ContentCategory.PROGRAMME_GROUP);

        DBCursor found = seriesCollection.find(where().fieldEquals(ItemTranslator.CONTAINER, containerUri).build());
        if (found != null) {
            for (DBObject dbo : found) {
                Identified model = toModel(dbo);
                results.put(model.getCanonicalUri(), model);
            }
        }

        ImmutableMap<String, Identified> res = results.build();
        addIdsToResults(res);
        return ResolvedContent.builder().putAll(res).build();
    }

    private void addIdsToResults(ImmutableMap<String, Identified> uriToIdentified) {
        Map<String, Long> idsForCanonicalUris = lookupEntryStore.idsForCanonicalUris(uriToIdentified.keySet());
        
        for(Entry<String, Identified> result : uriToIdentified.entrySet()) {
            Long id = idsForCanonicalUris.get(result.getKey());
            if (id == null) {
                log.info("null id for {}, {}, {}", new Object[]{result.getKey(), uriToIdentified.keySet(), idsForCanonicalUris});
            }
            result.getValue().setId(id);
        }
    }

    private Identified toModel(DBObject dbo) {
        if(dbo == null) {
            return null;
        }
        if (!dbo.containsField(DescribedTranslator.TYPE_KEY)) {
            throw new IllegalStateException("Missing type field");
        }
        EntityType type = EntityType.from((String) dbo.get(DescribedTranslator.TYPE_KEY));
        
        switch(type) {
        	case BRAND:
        	case SERIES:
        	case CONTAINER:
        		return containerTranslator.fromDBObject(dbo, null);
        	case ITEM:
        	case EPISODE:
        	case CLIP:
        	case FILM:
        	case SONG:
        		return itemTranslator.fromDBObject(dbo, null);
        }
        throw new IllegalArgumentException("Unknown type: " + type);
    }

    /**
     * DON'T USE THIS!!!
     * Very slow. Using temporarily in ExactTitleGenerator as a direct db query for short titles.
     */
    @Deprecated
    public ResolvedContent getExactTitleMatches(Content content) {
        BasicDBObject object = new BasicDBObject();
        object.append("publisher", content.getPublisher().key());
        object.append("title", content.getTitle());


        LookupRef ref = LookupRef.from(content);
        DBCursor cursor = contentTables.collectionFor(ref.category()).find(object);

        ResolvedContent.ResolvedContentBuilder builder = ResolvedContent.builder();
        while(cursor.hasNext()) {
            Identified iden = toModel(cursor.next());
            builder.put(iden.getCanonicalUri(), iden);
        }

        return builder.build();
    }
}
