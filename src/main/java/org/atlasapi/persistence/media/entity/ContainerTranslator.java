package org.atlasapi.persistence.media.entity;

import java.util.List;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.ChildRef;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.EntityType;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Series;
import org.atlasapi.persistence.ModelTranslator;
import org.joda.time.DateTime;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class ContainerTranslator implements ModelTranslator<Container<?>> {

	private static final String CHILDREN_KEY = "childRefs";
    private static final String SERIES_NUMBER_KEY = "seriesNumber";

    private static final String FULL_SERIES_KEY = "series";

    private final ContentTranslator contentTranslator;

    public ContainerTranslator() {
        this.contentTranslator = new ContentTranslator();
    }

    public List<Container<?>> fromDBObjects(Iterable<DBObject> dbObjects) {
        ImmutableList.Builder<Container<?>> containers = ImmutableList.builder();

        if (dbObjects != null) {
            for (DBObject dbObject : dbObjects) {
                containers.add(fromDB(dbObject));
            }
        }

        return containers.build();
    }

    public Container<?> fromDB(DBObject dbObject) {
        return fromDBObject(dbObject, null);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Container<?> fromDBObject(DBObject dbObject, Container<?> entity) {
        if (entity == null) {
            entity = (Container<?>) DescribedTranslator.newModel(dbObject);
        }
        contentTranslator.fromDBObject(dbObject, entity);

        if (dbObject.containsField(CHILDREN_KEY)) {
            Iterable<DBObject> childrenDBos = (Iterable<DBObject>) dbObject.get(CHILDREN_KEY);
            ((Container<Item>) entity).setChildRefs(Iterables.transform(childrenDBos, TO_CHILD_REF));
        }

        if (entity instanceof Series) {
            Series series = (Series) entity;
            series.withSeriesNumber((Integer) dbObject.get(SERIES_NUMBER_KEY));
        }

        if (entity instanceof Brand) {
            ((Brand) entity).setSeriesRefs(series((Iterable<DBObject>) dbObject.get(FULL_SERIES_KEY)));
        }
        return entity;
    }

    private static final Function<DBObject, ChildRef> TO_CHILD_REF = new Function<DBObject, ChildRef>() {
        @Override
        public ChildRef apply(DBObject input) {
            String uri = (String) input.get("uri");
            String sortKey = (String) input.get("sortKey");
            DateTime updated = TranslatorUtils.toDateTime(input, "updated");
            EntityType type = EntityType.from((String) input.get(DescribedTranslator.TYPE_KEY));
            return new ChildRef(uri, sortKey, updated, type);
        }
    };

    private List<ChildRef> series(Iterable<DBObject> seriesDbos) {
        if (seriesDbos != null) {
            return ImmutableList.copyOf(Iterables.transform(seriesDbos, TO_CHILD_REF));
        }
        return ImmutableList.of();
    }
   
    public DBObject toDB(Container<?> entity) {
        return toDBObject(null, entity);
    }

    @Override
    public DBObject toDBObject(DBObject dbObject, Container<?> entity) {
        dbObject = toDboNotIncludingContents(dbObject, entity);
        dbObject.put(CHILDREN_KEY, Iterables.transform(entity.getChildRefs(), FROM_CHILD_REF));
        if (entity instanceof Brand) {
            Brand brand = (Brand) entity;
            if (!brand.getSeriesRefs().isEmpty()) {
                dbObject.put(FULL_SERIES_KEY, Iterables.transform(brand.getSeriesRefs(), FROM_CHILD_REF));
            }
        }
        return dbObject;
    }

    private static Function<ChildRef, DBObject> FROM_CHILD_REF = new Function<ChildRef, DBObject>() {
        @Override
        public DBObject apply(ChildRef input) {
            DBObject dbObject = new BasicDBObject();
            TranslatorUtils.from(dbObject, "uri", input.getUri());
            TranslatorUtils.from(dbObject, "sortKey", input.getSortKey());
            TranslatorUtils.fromDateTime(dbObject, "updated", input.getUpdated());
            dbObject.put(DescribedTranslator.TYPE_KEY, input.getType().toString());
            return dbObject;
        }
    };

    private DBObject toDboNotIncludingContents(DBObject dbObject, Container<?> entity) {
        dbObject = contentTranslator.toDBObject(dbObject, entity);
        dbObject.put(DescribedTranslator.TYPE_KEY, EntityType.from(entity));

        if (entity instanceof Series) {
            Series series = (Series) entity;
            if (series.getSeriesNumber() != null) {
                dbObject.put(SERIES_NUMBER_KEY, series.getSeriesNumber());
            }
        }
        return dbObject;
    }
}
