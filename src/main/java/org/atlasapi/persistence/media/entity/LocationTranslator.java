package org.atlasapi.persistence.media.entity;

import java.util.List;

import org.atlasapi.media.TransportSubType;
import org.atlasapi.media.TransportType;
import org.atlasapi.media.entity.Image;
import org.atlasapi.media.entity.Location;
import org.atlasapi.media.entity.Policy;
import org.atlasapi.media.entity.Quality;
import org.atlasapi.persistence.ModelTranslator;

import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class LocationTranslator implements ModelTranslator<Location> {
	
    public static final String POLICY = "policy";
    private static final String QUALITY = "quality";
    private static final String SUBTITLED_LANGUAGES = "subtitledLanguages";
    private static final String REQUIRED_ENCRYPTION = "requiredEncryption";
    private static final String VAT = "vat";

    private final IdentifiedTranslator descriptionTranslator = new IdentifiedTranslator();
	private final PolicyTranslator policyTranslator = new PolicyTranslator();

    @Override
    public Location fromDBObject(DBObject dbObject, Location entity) {
       
    	if (entity == null) {
            entity = new Location();
        }
        
        descriptionTranslator.fromDBObject(dbObject, entity);

        entity.setAvailable((Boolean) dbObject.get("available"));
        entity.setEmbedCode((String) dbObject.get("embedCode"));
        entity.setEmbedId((String) dbObject.get("embedId"));
        entity.setTransportIsLive((Boolean) dbObject.get("transportIsLive"));
        
        entity.setTransportType(readEnum(TransportType.class, dbObject, "transportType"));
        entity.setTransportSubType(readEnum(TransportSubType.class, dbObject, "transportSubType"));
        
        entity.setUri((String) dbObject.get("uri"));
        entity.setLastUpdated(TranslatorUtils.toDateTime(dbObject, IdentifiedTranslator.LAST_UPDATED));
        entity.setRequiredEncryption(TranslatorUtils.toBoolean(dbObject, REQUIRED_ENCRYPTION));
        entity.setVat(TranslatorUtils.toDouble(dbObject, VAT));

        decodeSubtitledLanguages(dbObject, entity);

        DBObject policyObject = (DBObject) dbObject.get(POLICY);
        if (policyObject != null) {
        	entity.setPolicy(policyTranslator.fromDBObject(policyObject, new Policy()));
        }
        return entity;
    }

    protected void decodeSubtitledLanguages(DBObject dbObject, Location entity) {
        if(dbObject.containsField(SUBTITLED_LANGUAGES)) {
            entity.setSubtitledLanguages(TranslatorUtils.toSet(dbObject, SUBTITLED_LANGUAGES));
        }

    }

    private <T extends Enum<T>> T readEnum(Class<T> clazz, DBObject dbObject, String field) {
    	String value = (String) dbObject.get(field);
    	if (value == null) { 
    		return null;
    	}
		return Enum.valueOf(clazz, value.toUpperCase());
	}

	@Override
    public DBObject toDBObject(DBObject dbObject, Location entity) {
        dbObject = descriptionTranslator.toDBObject(dbObject, entity);
        
        TranslatorUtils.from(dbObject, "available", entity.getAvailable());
        TranslatorUtils.from(dbObject, "embedCode", entity.getEmbedCode());
        TranslatorUtils.from(dbObject, "embedId", entity.getEmbedId());
        TranslatorUtils.from(dbObject, "transportIsLive", entity.getTransportIsLive());
        TranslatorUtils.from(dbObject, REQUIRED_ENCRYPTION, entity.getRequiredEncryption());
        TranslatorUtils.from(dbObject, VAT, entity.getVat());

        encodeSubtitledLanguages(dbObject, entity);

        if (entity.getTransportType() != null) {
        	TranslatorUtils.from(dbObject, "transportType", entity.getTransportType().toString());
        }
        
        if (entity.getTransportSubType() != null) {
        	TranslatorUtils.from(dbObject, "transportSubType", entity.getTransportSubType().toString());
        }
        
        TranslatorUtils.from(dbObject, "uri", entity.getUri());
        TranslatorUtils.fromDateTime(dbObject, IdentifiedTranslator.LAST_UPDATED, entity.getLastUpdated());
        
        if (entity.getPolicy() != null) {
        	DBObject policyObject = policyTranslator.toDBObject(new BasicDBObject(), entity.getPolicy());
			if (!policyObject.keySet().isEmpty()) {
				dbObject.put(POLICY, policyObject);
			}
        }
        return dbObject;
    }

    protected void encodeSubtitledLanguages(DBObject dbObject, Location entity) {
        if(!entity.getSubtitledLanguages().isEmpty()) {
            TranslatorUtils.fromSet(dbObject, entity.getSubtitledLanguages(), SUBTITLED_LANGUAGES);
        }
    }
}
