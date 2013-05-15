package org.atlasapi.persistence.media.entity;

import java.util.List;

import org.atlasapi.media.entity.SeriesRef;
import org.joda.time.DateTime;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBList;
import com.mongodb.DBObject;

import static org.atlasapi.persistence.media.entity.ChildRefTranslator.URI_KEY;
import static org.atlasapi.persistence.media.entity.ChildRefTranslator.SORT_KEY;
import static org.atlasapi.persistence.media.entity.ChildRefTranslator.UPDATED_KEY;

public class SeriesRefTranslator {

    private static final String SERIES_NUMBER_KEY = "seriesNumber";
    
    private final ChildRefTranslator childRefTranslator = new ChildRefTranslator();
    
    public SeriesRef fromDBObject(DBObject dbo) {
        String uri = (String) dbo.get(URI_KEY);
        DateTime updated = TranslatorUtils.toDateTime(dbo, UPDATED_KEY);
        String sortKey = (String) dbo.get(SORT_KEY);
        Integer seriesNumber = TranslatorUtils.toInteger(dbo, SERIES_NUMBER_KEY);
        return new SeriesRef(uri, sortKey, seriesNumber, updated);
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
        DBObject dbObject = childRefTranslator.toDBObject(seriesRef);
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
