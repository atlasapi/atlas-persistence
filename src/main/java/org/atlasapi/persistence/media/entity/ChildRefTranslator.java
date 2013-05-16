package org.atlasapi.persistence.media.entity;

import java.util.List;

import org.atlasapi.media.entity.ChildRef;
import org.atlasapi.media.entity.EntityType;
import org.joda.time.DateTime;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class ChildRefTranslator {

    public static final String UPDATED_KEY = "updated";
    public static final String SORT_KEY = "sortKey";
    public static final String URI_KEY = "uri";
    public static final String ID_KEY = "id";

    public ChildRef fromDBObject(DBObject dbo) {
        Long id = (Long) dbo.get(ID_KEY);
        String uri = (String) dbo.get(URI_KEY);
        String sortKey = (String) dbo.get(SORT_KEY);
        DateTime updated = TranslatorUtils.toDateTime(dbo, UPDATED_KEY);
        EntityType type = EntityType.from((String) dbo.get(DescribedTranslator.TYPE_KEY));
        return new ChildRef(id, uri, sortKey, updated, type);
    }
    
    public List<ChildRef> fromDBObjects(Iterable<DBObject> dbos) {
        return ImmutableList.<ChildRef>copyOf(Iterables.transform(dbos, TO_CHILD_REF));
    }

    private final Function<DBObject, ChildRef> TO_CHILD_REF = new Function<DBObject, ChildRef>() {
        @Override
        public ChildRef apply(DBObject input) {
            return fromDBObject(input);
        }
    };

    public DBObject toDBObject(ChildRef childRef) {
        DBObject dbObject = new BasicDBObject();
        TranslatorUtils.from(dbObject, ID_KEY, childRef.getId());
        TranslatorUtils.from(dbObject, URI_KEY, childRef.getUri());
        TranslatorUtils.from(dbObject, SORT_KEY, childRef.getSortKey());
        TranslatorUtils.fromDateTime(dbObject, UPDATED_KEY, childRef.getUpdated());
        TranslatorUtils.from(dbObject, "type", childRef.getType().toString());
        return dbObject;
    }
    
    public BasicDBList toDBList(Iterable<ChildRef> childRefs) {
        BasicDBList list = new BasicDBList();
        list.addAll(ImmutableList.copyOf(Iterables.transform(childRefs, FROM_CHILD_REF)));
        return list;
    }

    private Function<ChildRef, DBObject> FROM_CHILD_REF = new Function<ChildRef, DBObject>() {
        @Override
        public DBObject apply(ChildRef input) {
            return toDBObject(input);
        }

    };

}
