package org.atlasapi.persistence.lookup.mongo;

import static com.metabroadcast.common.persistence.mongo.MongoConstants.ID;

import java.util.List;
import java.util.Set;

import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.joda.time.DateTime;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class LookupEntryTranslator {

    private static final String SELF = "self";
    private static final String DIRECT = "direct";
    private static final String EQUIVS = "equivs";
    private static final String PUBLISHER = "publisher";
    private static final String TYPE = "type";
    private static final String LAST_UPDATED = "updated";
    private static final String FIRST_CREATED = "created";
    private static final String ALIASES = "aliases";
    
    public Function<LookupEntry, DBObject> TO_DBO = new Function<LookupEntry, DBObject>() {
        @Override
        public DBObject apply(LookupEntry input) {
            return toDbo(input);
        }
    };
    
    public DBObject toDbo(LookupEntry entry) {
        BasicDBObject dbo = new BasicDBObject();
        
        TranslatorUtils.from(dbo, ID, entry.id());
        TranslatorUtils.from(dbo, SELF, equivalentToDbo.apply(entry.lookupRef()));
        TranslatorUtils.fromSet(dbo, entry.aliases(), ALIASES);
        
        BasicDBList directEquivDbos = new BasicDBList();
        directEquivDbos.addAll(ImmutableSet.copyOf(Iterables.transform(entry.directEquivalents(),equivalentToDbo)));
        TranslatorUtils.from(dbo, DIRECT, directEquivDbos);
        
        BasicDBList equivDbos = new BasicDBList();
        equivDbos.addAll(Lists.transform(entry.equivalents(), equivalentToDbo));
        TranslatorUtils.from(dbo, EQUIVS, equivDbos);
        
        TranslatorUtils.fromDateTime(dbo, FIRST_CREATED, entry.created());
        TranslatorUtils.fromDateTime(dbo, LAST_UPDATED, entry.updated());
        
        return dbo;
    }
    
    private static Function<LookupRef, DBObject> equivalentToDbo = new Function<LookupRef, DBObject>() {
        @Override
        public DBObject apply(LookupRef input) {
            BasicDBObject dbo = new BasicDBObject();
            
            TranslatorUtils.from(dbo, ID, input.id());
            TranslatorUtils.from(dbo, PUBLISHER, input.publisher().key());
            TranslatorUtils.from(dbo, TYPE, input.type().toString());
            
            return dbo;
        }
    };
    
    public LookupEntry fromDbo(DBObject dbo) {
        if(dbo == null) {
            return null;
        }
        
        String id = TranslatorUtils.toString(dbo, ID);
        Set<String> aliases = TranslatorUtils.toSet(dbo, ALIASES);
        LookupRef self = equivalentFromDbo.apply(TranslatorUtils.toDBObject(dbo, SELF));
        List<LookupRef> equivs = Lists.transform(TranslatorUtils.toDBObjectList(dbo, EQUIVS), equivalentFromDbo);
        DateTime created = TranslatorUtils.toDateTime(dbo, FIRST_CREATED);
        DateTime updated = TranslatorUtils.toDateTime(dbo, LAST_UPDATED);
        
        Set<LookupRef> directEquivalents = ImmutableSet.copyOf(Iterables.transform(TranslatorUtils.toDBObjectList(dbo, DIRECT), equivalentFromDbo));
        
        return new LookupEntry(id, self, aliases, directEquivalents, equivs, created, updated);
    }

    private static final Function<DBObject, LookupRef> equivalentFromDbo = new Function<DBObject, LookupRef>() {
        @Override
        public LookupRef apply(DBObject input) {
            String id = TranslatorUtils.toString(input, ID);
            Publisher publisher = Publisher.fromKey(TranslatorUtils.toString(input, PUBLISHER)).requireValue();
            String type = TranslatorUtils.toString(input, TYPE);
            return new LookupRef(id, publisher, LookupRef.LookupType.valueOf(type));
        }
    };
}
