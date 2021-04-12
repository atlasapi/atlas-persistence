package org.atlasapi.persistence.media.entity;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Ordering;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.DBObject;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.ChildRef;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.EntityType;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.media.entity.Series;
import org.atlasapi.media.entity.SeriesRef;
import org.atlasapi.media.entity.SortKey;
import org.atlasapi.persistence.content.mongo.DbObjectHashCodeDebugger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.List;
import java.util.Set;

public class ContainerTranslator {


    private static final Logger log = LoggerFactory.getLogger(ContainerTranslator.class);
    
    public static final String CONTAINER = "container";
	public static final String CONTAINER_ID = "containerId";
    public static final String CHILDREN_KEY = "childRefs";
    public static final String SERIES_NUMBER_KEY = "seriesNumber";
    public static final String TOTAL_EPISODES = "totalEpisodes";
    public static final String FULL_SERIES_KEY = "series";

    public static final Set<String> DB_KEYS = ImmutableSet.of(
            CONTAINER,
            CONTAINER_ID,
            CHILDREN_KEY,
            SERIES_NUMBER_KEY,
            TOTAL_EPISODES,
            FULL_SERIES_KEY
    );

    private final ContentTranslator contentTranslator;
    private final ChildRefTranslator childRefTranslator;
    private final SeriesRefTranslator seriesRefTranslator;
    private final DbObjectHashCodeDebugger dboHashCodeDebugger = new DbObjectHashCodeDebugger();
    
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
                containers.add(fromDB(dbObject, false));
            }
        }

        return containers.build();
    }

    public Container fromDB(DBObject dbObject) {
        return fromDBObject(dbObject, null, false);
    }
    
    /**
     * 
     * @param dbObject
     * @param includeChildrenInHashCode should references to children be included in hash calculations.
     *              Generally this should be false, except where ChildRefs need to be maintained.
     * @return
     */
    public Container fromDB(DBObject dbObject, boolean includeChildrenInHashCode) {
        return fromDBObject(dbObject, null, includeChildrenInHashCode);
    }

    public Container fromDBObject(DBObject dbObject, Container entity) {
        return fromDBObject(dbObject, entity, false);
    }
    
    @SuppressWarnings("unchecked")
    public Container fromDBObject(DBObject dbObject, Container entity, boolean includeChildrenInHashCode) {
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
            Long containerId = TranslatorUtils.toLong(dbObject, CONTAINER_ID);
            if(dbObject.containsField(CONTAINER)) {
                series.setParentRef(new ParentRef((String)dbObject.get(CONTAINER), containerId));
            }
            series.setTotalEpisodes(TranslatorUtils.toInteger(dbObject, TOTAL_EPISODES));
        }

        if (entity instanceof Brand) {
            ((Brand) entity).setSeriesRefs(series((Iterable<DBObject>) dbObject.get(FULL_SERIES_KEY)));
        }
        
        entity.setReadHash(generateHashByRemovingFieldsFromTheDbo(dbObject, includeChildrenInHashCode));
        return entity;
    }

    private String generateHashByRemovingFieldsFromTheDbo(DBObject dbObject, boolean includeChildren) {
        dbObject.removeField(CONTAINER_ID);
        contentTranslator.removeFieldsForHash(dbObject);
        
        if (!includeChildren) {
            dbObject.removeField(CHILDREN_KEY);
            dbObject.removeField(FULL_SERIES_KEY);
        }
        
        if (log.isTraceEnabled()) {
            dboHashCodeDebugger.logHashCodes(dbObject, log);
        }
        return String.valueOf(dbObject.hashCode());
    }
    
    public String hashCodeOf(Container container) {
        return hashCodeOf(container, false);
    }
    
    public String hashCodeOf(Container container, boolean includeChildren) {
        return generateHashByRemovingFieldsFromTheDbo(toDBO(container, true), includeChildren);
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
                dbObject.put(CONTAINER, series.getParent().getUri());
                dbObject.put(CONTAINER_ID, series.getParent().getId());
            }
            if(series.getTotalEpisodes() != null) {
                TranslatorUtils.from(dbObject, TOTAL_EPISODES, series.getTotalEpisodes());
            }
        }
        return dbObject;
    }
}
