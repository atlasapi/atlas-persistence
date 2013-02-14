package org.atlasapi.persistence.media.entity;

import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.EntityType;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Image;
import org.atlasapi.media.entity.ImageAspectRatio;
import org.atlasapi.media.entity.ImageType;
import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Specialization;
import org.atlasapi.persistence.ModelTranslator;
import org.joda.time.DateTime;

import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.media.MimeType;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class DescribedTranslator implements ModelTranslator<Described> {

    public static final String PUBLISHER_KEY = "publisher";
    private static final String SCHEDULE_ONLY_KEY = "scheduleOnly";
    public static final String THIS_OR_CHILD_LAST_UPDATED_KEY = "thisOrChildLastUpdated";
    public static final String TYPE_KEY = "type";
	public static final String LAST_FETCHED_KEY = "lastFetched";
    public static final String SHORT_DESC_KEY = "shortDescription";
    public static final String MEDIUM_DESC_KEY = "mediumDescription";
    public static final String LONG_DESC_KEY = "longDescription";
	
	private final IdentifiedTranslator descriptionTranslator;
    

	public DescribedTranslator(IdentifiedTranslator descriptionTranslator) {
		this.descriptionTranslator = descriptionTranslator;
	}
	
	@Override
	public Described fromDBObject(DBObject dbObject, Described entity) {

		descriptionTranslator.fromDBObject(dbObject, entity);

		entity.setDescription((String) dbObject.get("description"));
		
		entity.setShortDescription(TranslatorUtils.toString(dbObject, SHORT_DESC_KEY));
		entity.setMediumDescription(TranslatorUtils.toString(dbObject, MEDIUM_DESC_KEY));
		entity.setLongDescription(TranslatorUtils.toString(dbObject, LONG_DESC_KEY));

		entity.setFirstSeen(TranslatorUtils.toDateTime(dbObject, "firstSeen"));
		entity.setThisOrChildLastUpdated(TranslatorUtils.toDateTime(dbObject, THIS_OR_CHILD_LAST_UPDATED_KEY));

		entity.setGenres(TranslatorUtils.toSet(dbObject, "genres"));
		entity.setImage((String) dbObject.get("image"));
		entity.setLastFetched(TranslatorUtils.toDateTime(dbObject, LAST_FETCHED_KEY));
		Boolean scheduleOnly = TranslatorUtils.toBoolean(dbObject, SCHEDULE_ONLY_KEY);
		entity.setScheduleOnly(scheduleOnly != null ? scheduleOnly : false);

		String publisherKey = (String) dbObject.get(PUBLISHER_KEY);
		if (publisherKey != null) {
			entity.setPublisher(Publisher.fromKey(publisherKey).valueOrDefault(null));
		}
		entity.setImages(toImages(entity.getImage(), entity.getPublisher()));

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
		
		entity.setPresentationChannel(TranslatorUtils.toString(dbObject, "presentationChannel"));

		return entity;
	}

	private Iterable<Image> toImages(String imageUri, Publisher publisher) {
	    if(imageUri == null) {
	        return ImmutableSet.of();
	    }
	    
	    Image image = new Image(imageUri);
	    
	    if(Publisher.PA.equals(publisher)) {
	        image.setHeight(360);
	        image.setWidth(640);
	        image.setType(ImageType.PRIMARY);
	        image.setAspectRatio(ImageAspectRatio.SIXTEEN_BY_NINE);
	        image.setMimeType(MimeType.IMAGE_JPG);
	        image.setCanonicalUri(imageUri.replace("images.atlasapi.org/pa/", "images.atlas.metabroadcast.com/pressassociation.com/"));
	        image.setAvailabilityStart(null);
	        image.setAvailabilityStart(null);
	    }
	    
	    return ImmutableSet.of(image);
    }

    @Override
	public DBObject toDBObject(DBObject dbObject, Described entity) {
		 if (dbObject == null) {
            dbObject = new BasicDBObject();
         }
		 
	    descriptionTranslator.toDBObject(dbObject, entity);

	    TranslatorUtils.from(dbObject, SHORT_DESC_KEY, entity.getShortDescription());
	    TranslatorUtils.from(dbObject, MEDIUM_DESC_KEY, entity.getMediumDescription());
	    TranslatorUtils.from(dbObject, LONG_DESC_KEY, entity.getLongDescription());

        TranslatorUtils.from(dbObject, "description", entity.getDescription());
        TranslatorUtils.fromDateTime(dbObject, "firstSeen", entity.getFirstSeen());
        TranslatorUtils.fromDateTime(dbObject, THIS_OR_CHILD_LAST_UPDATED_KEY, entity.getThisOrChildLastUpdated());
        TranslatorUtils.fromSet(dbObject, entity.getGenres(), "genres");
        TranslatorUtils.from(dbObject, "image", entity.getImage());
        TranslatorUtils.fromDateTime(dbObject, LAST_FETCHED_KEY, entity.getLastFetched());
        
        if (entity.getPublisher() != null) {
        	TranslatorUtils.from(dbObject, PUBLISHER_KEY, entity.getPublisher().key());
        }
        
        TranslatorUtils.fromSet(dbObject, entity.getTags(), "tags");
        TranslatorUtils.from(dbObject, "thumbnail", entity.getThumbnail());
        TranslatorUtils.from(dbObject, "title", entity.getTitle());
        
        if (entity.isScheduleOnly()) {
            dbObject.put(SCHEDULE_ONLY_KEY, Boolean.TRUE);
        }
        
        if (entity.getMediaType() != null) {
        	TranslatorUtils.from(dbObject, "mediaType", entity.getMediaType().toString().toLowerCase());
        }
        if (entity.getSpecialization() != null) {
            TranslatorUtils.from(dbObject, "specialization", entity.getSpecialization().toString().toLowerCase());
        }
        
        TranslatorUtils.from(dbObject, "presentationChannel", entity.getPresentationChannel());
        
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
