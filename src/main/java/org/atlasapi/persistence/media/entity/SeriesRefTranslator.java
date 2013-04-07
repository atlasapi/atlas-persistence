package org.atlasapi.persistence.media.entity;

import java.util.List;

import org.atlasapi.media.common.Id;
import org.atlasapi.media.entity.SeriesRef;
import org.joda.time.DateTime;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import static org.atlasapi.persistence.media.entity.ChildRefTranslator.ID_KEY;
import static org.atlasapi.persistence.media.entity.ChildRefTranslator.SORT_KEY;
import static org.atlasapi.persistence.media.entity.ChildRefTranslator.UPDATED_KEY;

public class SeriesRefTranslator {

    private static final String SERIES_NUMBER_KEY = "seriesNumber";
    
    public SeriesRef fromDBObject(DBObject dbo) {
        Long id = (Long) dbo.get(ID_KEY);
        DateTime updated = TranslatorUtils.toDateTime(dbo, UPDATED_KEY);
        String sortKey = (String) dbo.get(SORT_KEY);
        Integer seriesNumber = TranslatorUtils.toInteger(dbo, SERIES_NUMBER_KEY);
        return new SeriesRef(Id.valueOf(id), sortKey, seriesNumber, updated);
    }
    
    public List<SeriesRef> fromDBObjects(Iterable<DBObject> dbos) {
        return ImmutableList.<SeriesRef>copyOf(Iterables.transform(dbos, TO_SERIES_REF));
    }
    
    private final Function<DBObject, SeriesRef> TO_SERIES_REF = new Function<DBObject, SeriesRef>() {
        @Override
        public SeriesRef apply(DBObject input) {
            return fromDBObject(input);
        }
    };
    
    public DBObject toDBObject(SeriesRef seriesRef) {
        DBObject dbObject = new BasicDBObject();
        TranslatorUtils.from(dbObject, ID_KEY, seriesRef.getId());
        TranslatorUtils.from(dbObject, SORT_KEY, seriesRef.getTitle());
        TranslatorUtils.fromDateTime(dbObject, UPDATED_KEY, seriesRef.getUpdated());
        TranslatorUtils.from(dbObject, SERIES_NUMBER_KEY, seriesRef.getSeriesNumber());
        return dbObject;
    }
    
    public BasicDBList toDBList(Iterable<SeriesRef> seriesRefs) {
        BasicDBList list = new BasicDBList();
        list.addAll(ImmutableList.copyOf(Iterables.transform(seriesRefs, FROM_SERIES_REF)));
        return list;
    }
    
    private Function<SeriesRef, DBObject> FROM_SERIES_REF = new Function<SeriesRef, DBObject>() {
        @Override
        public DBObject apply(SeriesRef input) {
            return toDBObject(input);
        }
    };
}
