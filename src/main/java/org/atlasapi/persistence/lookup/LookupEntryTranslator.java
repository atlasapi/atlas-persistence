package org.atlasapi.persistence.lookup;

import static com.metabroadcast.common.persistence.mongo.MongoConstants.ID;

import java.util.List;
import java.util.Set;

import org.atlasapi.media.entity.Publisher;
import org.joda.time.DateTime;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class LookupEntryTranslator {

    private static final String DIRECT = "direct";
    private static final String EQUIVS = "equivs";
    private static final String PUBLISHER = "publisher";
    private static final String TYPE = "type";
    private static final String LAST_UPDATED = "updated";
    private static final String FIRST_CREATED = "created";
    private static final String ALIASES = "aliases";
    
    public DBObject toDbo(LookupEntry entry) {
        BasicDBObject dbo = new BasicDBObject();
        
        TranslatorUtils.from(dbo, ID, entry.id());
        TranslatorUtils.fromSet(dbo, entry.aliases(), ALIASES);
        TranslatorUtils.from(dbo, PUBLISHER, entry.publisher().key());
        TranslatorUtils.from(dbo, TYPE, entry.type());
        TranslatorUtils.fromSet(dbo, entry.directEquivalents(), DIRECT);
        
        BasicDBList equivDbos = new BasicDBList();
        equivDbos.addAll(Lists.transform(entry.equivalents(), equivalentToDbo));
        TranslatorUtils.from(dbo, EQUIVS, equivDbos);
        
        TranslatorUtils.fromDateTime(dbo, FIRST_CREATED, entry.created());
        TranslatorUtils.fromDateTime(dbo, LAST_UPDATED, entry.updated());
        
        return dbo;
    }
    
    private static Function<Equivalent, DBObject> equivalentToDbo = new Function<Equivalent, DBObject>() {
        @Override
        public DBObject apply(Equivalent input) {
            BasicDBObject dbo = new BasicDBObject();
            
            TranslatorUtils.from(dbo, ID, input.id());
            TranslatorUtils.from(dbo, PUBLISHER, input.publisher().key());
            TranslatorUtils.from(dbo, TYPE, input.type());
            
            return dbo;
        }
    };
    
    public LookupEntry fromDbo(DBObject dbo) {
        if(dbo == null) {
            return null;
        }
        
        String id = TranslatorUtils.toString(dbo, ID);
        Set<String> aliases = TranslatorUtils.toSet(dbo, ALIASES);
        Publisher publisher = Publisher.fromKey(TranslatorUtils.toString(dbo, PUBLISHER)).requireValue();
        String type = TranslatorUtils.toString(dbo, TYPE);
        List<Equivalent> equivs = Lists.transform(TranslatorUtils.toDBObjectList(dbo, EQUIVS), equivalentFromDbo);
        DateTime created = TranslatorUtils.toDateTime(dbo, FIRST_CREATED);
        DateTime updated = TranslatorUtils.toDateTime(dbo, LAST_UPDATED);
        
        return new LookupEntry(id, aliases, publisher, type, equivs, created, updated).withDirectEquivalents(TranslatorUtils.toSet(dbo, DIRECT));
    }

    private static final Function<DBObject, Equivalent> equivalentFromDbo = new Function<DBObject, Equivalent>() {
        @Override
        public Equivalent apply(DBObject input) {
            String id = TranslatorUtils.toString(input, ID);
            Publisher publisher = Publisher.fromKey(TranslatorUtils.toString(input, PUBLISHER)).requireValue();
            String type = TranslatorUtils.toString(input, TYPE);
            return new Equivalent(id , publisher , type);
        }
    };
}
