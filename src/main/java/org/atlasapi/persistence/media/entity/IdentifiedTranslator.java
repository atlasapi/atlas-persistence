package org.atlasapi.persistence.media.entity;

import java.util.Set;

import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.persistence.ModelTranslator;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.metabroadcast.common.persistence.mongo.MongoConstants;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class IdentifiedTranslator implements ModelTranslator<Identified> {
   
	public static final String CURIE = "curie";
	
	public static final String ALIASES = "aliases";
	public static final String IDS = "ids";
	public static final String LAST_UPDATED = "lastUpdated";
	public static final String EQUIVALENT_TO = "equivalent";
	public static final String ID = MongoConstants.ID;
	public static final String CANONICAL_URL = "uri";
	public static final String TYPE = "type";
    public static final String PUBLISHER = "publisher";
    public static final String OPAQUE_ID = "aid";
    
    private final AliasTranslator aliasTranslator = new AliasTranslator();
    private static final LookupRefTranslator lookupRefTranslator = new LookupRefTranslator();

	private boolean useAtlasIdAsId;
    
	public IdentifiedTranslator() {
		this(false);
	}
	
	public IdentifiedTranslator(boolean atlasIdAsId) {
		this.useAtlasIdAsId = atlasIdAsId;
	}
	
	@Override
    public Identified fromDBObject(DBObject dbObject, Identified description) {
        if (description == null) {
            description = new Identified();
        }
        
        if(useAtlasIdAsId) {
            description.setCanonicalUri((String) dbObject.get(CANONICAL_URL));
            description.setId((Long) dbObject.get(ID));
        }
        else {
            description.setCanonicalUri((String) dbObject.get(ID));
            try {
                description.setId((Long) dbObject.get(OPAQUE_ID));
            } catch (ClassCastException cce) {
                description.setId(((Double) dbObject.get(OPAQUE_ID)).longValue());
            }
        }
        
        description.setCurie((String) dbObject.get(CURIE));
        description.setAliasUrls(TranslatorUtils.toSet(dbObject, ALIASES));

        description.setAliases(aliasTranslator.fromDBObjects(TranslatorUtils.toDBObjectList(dbObject, IDS)));
        
        description.setEquivalentTo(equivalentsFrom(dbObject));
        description.setLastUpdated(TranslatorUtils.toDateTime(dbObject, LAST_UPDATED));
        return description;
    }

    private Set<LookupRef> equivalentsFrom(DBObject dbObject) {
        return Sets.newHashSet(Iterables.transform(TranslatorUtils.toDBObjectList(dbObject, EQUIVALENT_TO), equivalentFromDbo));
    }
	
    private static final Function<DBObject, LookupRef> equivalentFromDbo = new Function<DBObject, LookupRef>() {
        @Override
        public LookupRef apply(DBObject input) {
            return lookupRefTranslator.fromDBObject(input, null);
        }
    };

    @Override
    public DBObject toDBObject(DBObject dbObject, Identified entity) {
        if (dbObject == null) {
            dbObject = new BasicDBObject();
        }
        
        if (useAtlasIdAsId) {
            TranslatorUtils.from(dbObject, CANONICAL_URL, entity.getCanonicalUri());
            TranslatorUtils.from(dbObject, ID, entity.getId());
        }
        else {
            TranslatorUtils.from(dbObject, ID, entity.getCanonicalUri());
            TranslatorUtils.from(dbObject, OPAQUE_ID, entity.getId());
        }
        
        TranslatorUtils.from(dbObject, CURIE, entity.getCurie());
        TranslatorUtils.fromSet(dbObject, entity.getAliasUrls(), ALIASES);
        
        TranslatorUtils.from(dbObject, IDS, aliasTranslator.toDBList(entity.getAliases()));
        
        TranslatorUtils.from(dbObject, EQUIVALENT_TO, toDBObject(entity.getEquivalentTo()));
        TranslatorUtils.fromDateTime(dbObject, LAST_UPDATED, entity.getLastUpdated());
        
        return dbObject;
    }

    private BasicDBList toDBObject(Set<LookupRef> equivalentTo) {
        BasicDBList list = new BasicDBList();
        Iterables.addAll(list, Iterables.transform(equivalentTo, equivalentToDbo));
        return list;
    }
    
    private static Function<LookupRef, DBObject> equivalentToDbo = new Function<LookupRef, DBObject>() {
        @Override
        public DBObject apply(LookupRef input) {
            return lookupRefTranslator.toDBObject(null, input);
        }
    };
}
