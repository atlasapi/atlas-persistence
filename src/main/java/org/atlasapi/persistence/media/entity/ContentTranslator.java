package org.atlasapi.persistence.media.entity;

import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.ModelTranslator;

import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class ContentTranslator implements ModelTranslator<Content> {

	public static final String CONTAINED_IN_URIS_KEY = "containedInUris";
	
	private final DescriptionTranslator descriptionTranslator ;

	public ContentTranslator(DescriptionTranslator descriptionTranslator) {
		this.descriptionTranslator = descriptionTranslator;
	}
	
	@Override
	public Content fromDBObject(DBObject dbObject, Content entity) {
		    if (entity == null) {
			   entity = new Content();
	        }
		    
		    descriptionTranslator.fromDBObject(dbObject, entity);

	        entity.setContainedInUris(TranslatorUtils.toSet(dbObject, CONTAINED_IN_URIS_KEY));
	        
	        entity.setDescription((String) dbObject.get("description"));
	        
	        entity.setFirstSeen(TranslatorUtils.toDateTime(dbObject, "firstSeen"));
	        entity.setThisOrChildLastUpdated(TranslatorUtils.toDateTime(dbObject, "thisOrChildLastUpdated"));
	        
	        entity.setGenres(TranslatorUtils.toSet(dbObject, "genres"));
	        entity.setImage((String) dbObject.get("image"));
	        entity.setLastFetched(TranslatorUtils.toDateTime(dbObject, "lastFetched"));
	        
	        String publisherKey = (String) dbObject.get("publisher");
	        if (publisherKey != null) {
	        	entity.setPublisher(Publisher.fromKey(publisherKey).valueOrDefault(null));
	        }
	        
	        entity.setTags(TranslatorUtils.toSet(dbObject, "tags"));
	        entity.setThumbnail((String) dbObject.get("thumbnail"));
	        entity.setTitle((String) dbObject.get("title"));
		   
	       return entity;
	}

	@Override
	public DBObject toDBObject(DBObject dbObject, Content entity) {
		 if (dbObject == null) {
	            dbObject = new BasicDBObject();
	        }
		 
		    descriptionTranslator.toDBObject(dbObject, entity);

	        TranslatorUtils.fromSet(dbObject, entity.getContainedInUris(), CONTAINED_IN_URIS_KEY);
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

	        return dbObject;
	}

}
