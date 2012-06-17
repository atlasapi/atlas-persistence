package org.atlasapi.persistence.media.entity;

import java.util.List;

import org.atlasapi.media.content.ChildRef;
import org.atlasapi.media.content.EntityType;
import org.joda.time.DateTime;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class ChildRefTranslator {

    public ChildRef fromDBObject(DBObject dbo) {
        String uri = (String) dbo.get("uri");
        String sortKey = (String) dbo.get("sortKey");
        DateTime updated = TranslatorUtils.toDateTime(dbo, "updated");
        EntityType type = EntityType.from((String) dbo.get(DescribedTranslator.TYPE_KEY));
        return new ChildRef(uri, sortKey, updated, type);
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
        TranslatorUtils.from(dbObject, "uri", childRef.getUri());
        TranslatorUtils.from(dbObject, "sortKey", childRef.getSortKey());
        TranslatorUtils.fromDateTime(dbObject, "updated", childRef.getUpdated());
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
