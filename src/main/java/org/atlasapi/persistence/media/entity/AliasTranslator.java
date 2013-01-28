package org.atlasapi.persistence.media.entity;

import java.util.Set;

import org.atlasapi.media.entity.Alias;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class AliasTranslator {
    
    public static final String NAMESPACE = "namespace";
    public static final String VALUE = "value";

    public Set<Alias> fromDBObjects(Iterable<DBObject> dbos) {
        return ImmutableSet.<Alias>copyOf(Iterables.transform(dbos, TO_ALIAS));
    }
    
    public BasicDBList toDBList(Iterable<Alias> aliases) {
        BasicDBList list = new BasicDBList();
        list.addAll(ImmutableSet.copyOf(Iterables.transform(aliases, FROM_ALIAS)));
        return list;
    }

    private Function<Alias, DBObject> FROM_ALIAS = new Function<Alias, DBObject>() {
        @Override
        public DBObject apply(Alias alias) {
            return toDBObject(alias);
        }
    };
    
    public DBObject toDBObject(Alias alias) {
        DBObject dbo = new BasicDBObject();
        
        TranslatorUtils.from(dbo, NAMESPACE, alias.getNamespace());
        TranslatorUtils.from(dbo, VALUE, alias.getValue());
        
        return dbo;
    }

    private Function<DBObject, Alias> TO_ALIAS = new Function<DBObject, Alias>() {
        @Override
        public Alias apply(DBObject dbObject) {
            return fromDBObject(dbObject);
        }
    };

    public Alias fromDBObject(DBObject dbObject) {
        return new Alias(TranslatorUtils.toString(dbObject, NAMESPACE), TranslatorUtils.toString(dbObject, VALUE));
    }
}
