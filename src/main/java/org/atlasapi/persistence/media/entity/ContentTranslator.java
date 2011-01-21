package org.atlasapi.persistence.media.entity;

import java.util.List;

import org.atlasapi.media.entity.Clip;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Specialization;
import org.atlasapi.persistence.ModelTranslator;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class ContentTranslator implements ModelTranslator<Content> {

	public static final String CONTAINED_IN_URIS_KEY = "containedInUris";
	private static String CLIPS_KEY = "clips";
	
	private final DescriptionTranslator descriptionTranslator;
	private ClipTranslator clipTranslator;

	public ContentTranslator(DescriptionTranslator descriptionTranslator, ClipTranslator translator) {
		this.descriptionTranslator = descriptionTranslator;
		this.clipTranslator = translator;
	}
	
	public void setClipTranslator(ClipTranslator clipTranslator) {
		this.clipTranslator = clipTranslator;
	}
	
	@SuppressWarnings("unchecked")
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
        
        if (dbObject.containsField(CLIPS_KEY)) {
        	Iterable<DBObject> clipsDbos = (Iterable<DBObject>) dbObject.get(CLIPS_KEY);
        	Iterable<Clip> clips = Iterables.transform(clipsDbos, new Function<DBObject, Clip>() {

				@Override
				public Clip apply(DBObject dbo) {
					return clipTranslator.fromDBObject(dbo, null);
				}
        	});
        	entity.setClips(clips);
        }
        
        // Hack to transfer from the old name to the new
        String cType = (String)dbObject.get("contentType");
        if (cType != null) {
        	entity.setMediaType(MediaType.valueOf(cType.toUpperCase()));
        } else {
            cType = (String)dbObject.get("mediaType");
            if (cType != null) {
                entity.setMediaType(MediaType.valueOf(cType.toUpperCase()));
            }
        }
        
        String specialization = (String) dbObject.get("specialization");
        if (specialization != null) {
            entity.setSpecialization(Specialization.valueOf(specialization.toUpperCase()));
        }
        
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

        
        if (!entity.getClips().isEmpty()) {
        	List<DBObject> clipDbos = Lists.transform(entity.getClips(), new Function<Clip, DBObject>() {

				@Override
				public DBObject apply(Clip clip) {
					return clipTranslator.toDBObject(new BasicDBObject(), clip);
				}
			});
			dbObject.put(CLIPS_KEY, clipDbos);
        }
        
        if (entity.getMediaType() != null) {
        	TranslatorUtils.from(dbObject, "mediaType", entity.getMediaType().toString().toLowerCase());
        }
        if (entity.getSpecialization() != null) {
            TranslatorUtils.from(dbObject, "specialization", entity.getSpecialization().toString().toLowerCase());
        }
        
        return dbObject;
	}

}
