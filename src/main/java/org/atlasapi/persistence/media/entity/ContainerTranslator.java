package org.atlasapi.persistence.media.entity;

import java.util.Comparator;
import java.util.List;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.ChildRef;
import org.atlasapi.media.common.Id;
import org.atlasapi.media.content.Container;
import org.atlasapi.media.entity.EntityType;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.media.entity.Series;
import org.atlasapi.media.entity.SeriesRef;
import org.atlasapi.media.entity.SortKey;
import org.atlasapi.persistence.ModelTranslator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.DBObject;

public class ContainerTranslator implements ModelTranslator<Container> {

    public static final String CONTAINER_ID = "containerId";
    public static final String CHILDREN_KEY = "childRefs";
    private static final String SERIES_NUMBER_KEY = "seriesNumber";
    private static final String TOTAL_EPISODES = "totalEpisodes";

    public static final String FULL_SERIES_KEY = "series";

    private final ContentTranslator contentTranslator;
    private final ChildRefTranslator childRefTranslator;
    private final SeriesRefTranslator seriesRefTranslator;
    
    private static final Ordering<ChildRef> CHILD_REF_OUTPUT_SORT = Ordering.from(new Comparator<ChildRef>() {
        
        private Comparator<String> sortKeyComparator = new SortKey.SortKeyOutputComparator();
        
        @Override
        public int compare(ChildRef o1, ChildRef o2) {
            return sortKeyComparator.compare(o1.getSortKey(), o2.getSortKey());
            
        }
    });

    public ContainerTranslator(NumberToShortStringCodec idCodec) {
        this.contentTranslator = new ContentTranslator(idCodec);
        this.childRefTranslator = new ChildRefTranslator();
        this.seriesRefTranslator = new SeriesRefTranslator();
    }

    public List<Container> fromDBObjects(Iterable<DBObject> dbObjects) {
        ImmutableList.Builder<Container> containers = ImmutableList.builder();

        if (dbObjects != null) {
            for (DBObject dbObject : dbObjects) {
                containers.add(fromDB(dbObject));
            }
        }

        return containers.build();
    }

    public Container fromDB(DBObject dbObject) {
        return fromDBObject(dbObject, null);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Container fromDBObject(DBObject dbObject, Container entity) {
        if (entity == null) {
            entity = (Container) DescribedTranslator.newModel(dbObject);
        }
        contentTranslator.fromDBObject(dbObject, entity);

        Iterable<ChildRef> childRefs;
        if (dbObject.containsField(CHILDREN_KEY)) {
            childRefs = childRefTranslator.fromDBObjects((Iterable<DBObject>) dbObject.get(CHILDREN_KEY));
        } else {
            childRefs = ImmutableList.of();
        }
        entity.setChildRefs(CHILD_REF_OUTPUT_SORT.immutableSortedCopy(childRefs));

        if (entity instanceof Series) {
            Series series = (Series) entity;
            series.withSeriesNumber((Integer) dbObject.get(SERIES_NUMBER_KEY));
            if(dbObject.containsField(CONTAINER_ID)) {
                Id containerId = Id.valueOf(TranslatorUtils.toLong(dbObject, CONTAINER_ID));
                series.setParentRef(new ParentRef(containerId, EntityType.BRAND));
            }
            series.setTotalEpisodes(TranslatorUtils.toInteger(dbObject, TOTAL_EPISODES));
        }

        if (entity instanceof Brand) {
            ((Brand) entity).setSeriesRefs(series((Iterable<DBObject>) dbObject.get(FULL_SERIES_KEY)));
        }
        
        entity.setReadHash(generateHashByRemovingFieldsFromTheDbo(dbObject));
        return entity;
    }

    private String generateHashByRemovingFieldsFromTheDbo(DBObject dbObject) {
        // don't include the last-fetched time in the hash
        dbObject.removeField(DescribedTranslator.LAST_FETCHED_KEY);
        dbObject.removeField(DescribedTranslator.THIS_OR_CHILD_LAST_UPDATED_KEY);
        dbObject.removeField(IdentifiedTranslator.LAST_UPDATED);
        dbObject.removeField(CHILDREN_KEY);
        dbObject.removeField(FULL_SERIES_KEY);
        return String.valueOf(dbObject.hashCode());
    }
    
    public String hashCodeOf(Container container) {
        return generateHashByRemovingFieldsFromTheDbo(toDB(container));
    }

    private List<SeriesRef> series(Iterable<DBObject> seriesDbos) {
        if (seriesDbos != null) {
            return SeriesRef.dedupeAndSort(seriesRefTranslator.fromDBObjects(seriesDbos));
        }
        return ImmutableList.of();
    }
   
    public DBObject toDB(Container entity) {
        return toDBO(entity, false);
    }
    
    public DBObject toDBO(Container entity, boolean includeChildren) {
        DBObject dbObject = toDBObject(null, entity);
        if(includeChildren) {
          dbObject.put(CHILDREN_KEY, childRefTranslator.toDBList(entity.getChildRefs()));
          if (entity instanceof Brand) {
              Brand brand = (Brand) entity;
              if (!brand.getSeriesRefs().isEmpty()) {
                  dbObject.put(FULL_SERIES_KEY, seriesRefTranslator.toDBList(brand.getSeriesRefs()));
              }
          }
        }
        
        return dbObject;
    }

    @Override
    public DBObject toDBObject(DBObject dbObject, Container entity) {
        dbObject = toDboNotIncludingContents(dbObject, entity);
        return dbObject;
    }

    private DBObject toDboNotIncludingContents(DBObject dbObject, Container entity) {
        dbObject = contentTranslator.toDBObject(dbObject, entity);
        dbObject.put(DescribedTranslator.TYPE_KEY, EntityType.from(entity).toString());

        if (entity instanceof Series) {
            Series series = (Series) entity;
            if (series.getSeriesNumber() != null) {
                dbObject.put(SERIES_NUMBER_KEY, series.getSeriesNumber());
            }
            if (series.getParent() != null) {
                dbObject.put(CONTAINER_ID, series.getParent().getId().longValue());
            }
            if(series.getTotalEpisodes() != null) {
                TranslatorUtils.from(dbObject, TOTAL_EPISODES, series.getTotalEpisodes());
            }
        }
        return dbObject;
    }
}
