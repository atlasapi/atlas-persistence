package org.atlasapi.persistence.lookup.mongo;

import static com.metabroadcast.common.persistence.mongo.MongoConstants.ID;

import java.util.List;
import java.util.Set;

import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentCategory;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.media.entity.AliasTranslator;
import org.joda.time.DateTime;

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
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
    public static final String ALIASES = "aliases";
    public static final String IDS = "ids";
    public static final String OPAQUE_ID = "aid";
    
    private final AliasTranslator aliasTranslator = new AliasTranslator();
    
    public Function<LookupEntry, DBObject> TO_DBO = new Function<LookupEntry, DBObject>() {
        @Override
        public DBObject apply(LookupEntry input) {
            return toDbo(input);
        }
    };
    public Function<DBObject, LookupEntry> FROM_DBO = new Function<DBObject, LookupEntry>() {

        @Override
        public LookupEntry apply(DBObject dbo) {
            return fromDbo(dbo);
        }
    };
    
    public DBObject toDbo(LookupEntry entry) {
        BasicDBObject dbo = new BasicDBObject();
        
        TranslatorUtils.from(dbo, ID, entry.uri());
        TranslatorUtils.from(dbo, OPAQUE_ID, entry.id().longValue());
        TranslatorUtils.from(dbo, SELF, equivalentToDbo.apply(entry.lookupRef()));
        
        Set<String> aliases = Sets.newHashSet(entry.uri());
        aliases.addAll(entry.aliasUrls());
        TranslatorUtils.fromSet(dbo, aliases, ALIASES);
        
        TranslatorUtils.from(dbo, IDS, aliasTranslator.toDBList(entry.aliases()));
        
        BasicDBList directEquivDbos = new BasicDBList();
        directEquivDbos.addAll(ImmutableSet.copyOf(Iterables.transform(entry.directEquivalents(),equivalentToDbo)));
        TranslatorUtils.from(dbo, DIRECT, directEquivDbos);
        
        BasicDBList equivDbos = new BasicDBList();
        equivDbos.addAll(ImmutableSet.copyOf(Iterables.transform(entry.equivalents(), equivalentToDbo)));
        TranslatorUtils.from(dbo, EQUIVS, equivDbos);
        
        BasicDBList explicitDbos = new BasicDBList();
        explicitDbos.addAll(ImmutableSet.copyOf(Iterables.transform(entry.explicitEquivalents(),equivalentToDbo)));
        TranslatorUtils.from(dbo, "explicit", explicitDbos);
        
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
            TranslatorUtils.from(dbo, TYPE, input.category().toString());
            
            return dbo;
        }
    };
    
    public LookupEntry fromDbo(DBObject dbo) {
        if(dbo == null) {
            return null;
        }
        
        String uri = TranslatorUtils.toString(dbo, ID);
        Long id = TranslatorUtils.toLong(dbo, OPAQUE_ID);
        
        if (id == null) {
            return null;
        }
                
        Set<String> aliasUris = TranslatorUtils.toSet(dbo, ALIASES);
        aliasUris.add(uri);
        
        Set<Alias> aliases = aliasTranslator.fromDBObjects(TranslatorUtils.toDBObjectList(dbo, IDS));
        
        LookupRef self = equivalentFromDbo.apply(TranslatorUtils.toDBObject(dbo, SELF));
        Set<LookupRef> equivs = fromDbos(TranslatorUtils.toDBObjectList(dbo, EQUIVS));
        DateTime created = TranslatorUtils.toDateTime(dbo, FIRST_CREATED);
        DateTime updated = TranslatorUtils.toDateTime(dbo, LAST_UPDATED);
        
        Set<LookupRef> directEquivalents = fromDbos(TranslatorUtils.toDBObjectList(dbo, DIRECT));
        Set<LookupRef> explicitEquivalents = fromDbos(TranslatorUtils.toDBObjectList(dbo, "explicit"));
        
        return new LookupEntry(uri, id, self, aliasUris, aliases, directEquivalents, explicitEquivalents, equivs, created, updated);
    }
    
    private ImmutableSet<LookupRef> fromDbos(List<DBObject> equivRefs) {
        return ImmutableSet.copyOf(Iterables.filter(Iterables.transform(equivRefs, equivalentFromDbo), Predicates.notNull()));
    }

    private static final Function<DBObject, LookupRef> equivalentFromDbo = new Function<DBObject, LookupRef>() {
        @Override
        public LookupRef apply(DBObject input) {
            String id = TranslatorUtils.toString(input, ID);
            Publisher publisher = Publisher.fromKey(TranslatorUtils.toString(input, PUBLISHER)).requireValue();
            String type = TranslatorUtils.toString(input, TYPE);
            return new LookupRef(id, publisher, ContentCategory.valueOf(type));
        }
    };
}