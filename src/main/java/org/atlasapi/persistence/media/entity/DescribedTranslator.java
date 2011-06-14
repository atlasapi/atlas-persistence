package org.atlasapi.persistence.media.entity;

import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.EntityType;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Specialization;
import org.atlasapi.persistence.ModelTranslator;

import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class DescribedTranslator implements ModelTranslator<Described> {

    public static final String TYPE_KEY = "type";
	
	private final DescriptionTranslator descriptionTranslator;

	public DescribedTranslator(DescriptionTranslator descriptionTranslator) {
		this.descriptionTranslator = descriptionTranslator;
	}
	
	@Override
	public Described fromDBObject(DBObject dbObject, Described entity) {

		descriptionTranslator.fromDBObject(dbObject, entity);

		entity.setDescription((String) dbObject.get("description"));

		entity.setFirstSeen(TranslatorUtils.toDateTime(dbObject, "firstSeen"));
		entity.setThisOrChildLastUpdated(TranslatorUtils.toDateTime(dbObject, "thisOrChildLastUpdated"));

		entity.setGenres(TranslatorUtils.toSet(dbObject, "genres"));
		entity.setImage((String) dbObject.get("image"));
		entity.setLastFetched(TranslatorUtils.toDateTime(dbObject, "lastFetched"));
		Boolean scheduleOnly = TranslatorUtils.toBoolean(dbObject, "scheduleOnly");
		entity.setScheduleOnly(scheduleOnly != null ? scheduleOnly : false);

		String publisherKey = (String) dbObject.get("publisher");
		if (publisherKey != null) {
			entity.setPublisher(Publisher.fromKey(publisherKey).valueOrDefault(null));
		}

		entity.setTags(TranslatorUtils.toSet(dbObject, "tags"));
		entity.setThumbnail((String) dbObject.get("thumbnail"));
		entity.setTitle((String) dbObject.get("title"));

		String cType = (String) dbObject.get("mediaType");
		if (cType != null) {
			entity.setMediaType(MediaType.valueOf(cType.toUpperCase()));
		}

		String specialization = (String) dbObject.get("specialization");
		if (specialization != null) {
			entity.setSpecialization(Specialization.valueOf(specialization.toUpperCase()));
		}

		return entity;
	}

	@Override
	public DBObject toDBObject(DBObject dbObject, Described entity) {
		 if (dbObject == null) {
            dbObject = new BasicDBObject();
         }
		 
	    descriptionTranslator.toDBObject(dbObject, entity);

        TranslatorUtils.from(dbObject, "description", entity.getDescription());
        TranslatorUtils.fromDateTime(dbObject, "firstSeen", entity.getFirstSeen());
        TranslatorUtils.fromDateTime(dbObject, "thisOrChildLastUpdated", entity.getThisOrChildLastUpdated());
        TranslatorUtils.fromSet(dbObject, entity.getGenres(), "genres");
        TranslatorUtils.from(dbObject, "image", entity.getImage());
        TranslatorUtils.fromDateTime(dbObject, "lastFetched", entity.getLastFetched());
        
        if (entity.getPublisher() != null) {
        	TranslatorUtils.from(dbObject, "publisher", entity.getPublisher().key());
        }
        
        TranslatorUtils.fromSet(dbObject, entity.getTags(), "tags");
        TranslatorUtils.from(dbObject, "thumbnail", entity.getThumbnail());
        TranslatorUtils.from(dbObject, "title", entity.getTitle());
        dbObject.put("scheduleOnly", Boolean.valueOf(entity.isScheduleOnly()));
        
        if (entity.getMediaType() != null) {
        	TranslatorUtils.from(dbObject, "mediaType", entity.getMediaType().toString().toLowerCase());
        }
        if (entity.getSpecialization() != null) {
            TranslatorUtils.from(dbObject, "specialization", entity.getSpecialization().toString().toLowerCase());
        }
        
        return dbObject;
	}
	
	static Identified newModel(DBObject dbObject) {
		EntityType type = EntityType.from((String) dbObject.get(TYPE_KEY));
		try {
			return type.getModelClass().newInstance();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
