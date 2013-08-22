package org.atlasapi.persistence.media.entity;

import java.util.List;

import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.EntityType;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Image;
import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.RelatedLink;
import org.atlasapi.media.entity.Specialization;
import org.atlasapi.persistence.ModelTranslator;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class DescribedTranslator implements ModelTranslator<Described> {

    private static final String TAGS_KEY = "tags";
    private static final String THUMBNAIL_KEY = "thumbnail";
    private static final String TITLE_KEY = "title";
    private static final String MEDIA_TYPE_KEY = "mediaType";
    private static final String SPECIALIZATION_KEY = "specialization";
    private static final String PRESENTATION_CHANNEL_KEY = "presentationChannel";
    private static final String IMAGES_KEY = "images";
    private static final String IMAGE_KEY = "image";
    private static final String GENRES_KEY = "genres";
    private static final String FIRST_SEEN_KEY = "firstSeen";
    private static final String DESCRIPTION_KEY = "description";
    public static final String PUBLISHER_KEY = "publisher";
    private static final String SCHEDULE_ONLY_KEY = "scheduleOnly";
    public static final String THIS_OR_CHILD_LAST_UPDATED_KEY = "thisOrChildLastUpdated";
    public static final String TYPE_KEY = "type";
	public static final String LAST_FETCHED_KEY = "lastFetched";
    public static final String SHORT_DESC_KEY = "shortDescription";
    public static final String MEDIUM_DESC_KEY = "mediumDescription";
    public static final String LONG_DESC_KEY = "longDescription";
    private static final String ACTIVELY_PUBLISHED_KEY = "activelyPublished";
    private static final String LINKS_KEY = "links";
	
	private final IdentifiedTranslator descriptionTranslator;
    private final ImageTranslator imageTranslator;
    private final RelatedLinkTranslator relatedLinkTranslator;

	public DescribedTranslator(IdentifiedTranslator descriptionTranslator, ImageTranslator imageTranslator) {
		this.descriptionTranslator = descriptionTranslator;
		this.imageTranslator = imageTranslator;
        this.relatedLinkTranslator = new RelatedLinkTranslator();
	}
	
	@Override
	public Described fromDBObject(DBObject dbObject, Described entity) {

		descriptionTranslator.fromDBObject(dbObject, entity);

        decodeRelatedLinks(dbObject, entity);
		
		entity.setDescription((String) dbObject.get(DESCRIPTION_KEY));
		
		entity.setShortDescription(TranslatorUtils.toString(dbObject, SHORT_DESC_KEY));
		entity.setMediumDescription(TranslatorUtils.toString(dbObject, MEDIUM_DESC_KEY));
		entity.setLongDescription(TranslatorUtils.toString(dbObject, LONG_DESC_KEY));

		entity.setFirstSeen(TranslatorUtils.toDateTime(dbObject, FIRST_SEEN_KEY));
		entity.setThisOrChildLastUpdated(TranslatorUtils.toDateTime(dbObject, THIS_OR_CHILD_LAST_UPDATED_KEY));

		entity.setGenres(TranslatorUtils.toSet(dbObject, GENRES_KEY));
		entity.setImage((String) dbObject.get(IMAGE_KEY));
		entity.setLastFetched(TranslatorUtils.toDateTime(dbObject, LAST_FETCHED_KEY));
		Boolean scheduleOnly = TranslatorUtils.toBoolean(dbObject, SCHEDULE_ONLY_KEY);
		entity.setScheduleOnly(scheduleOnly != null ? scheduleOnly : false);

		String publisherKey = (String) dbObject.get(PUBLISHER_KEY);
		if (publisherKey != null) {
			entity.setPublisher(Publisher.fromKey(publisherKey).valueOrDefault(null));
		}
		
		List<DBObject> imagesDboList = TranslatorUtils.toDBObjectList(dbObject, IMAGES_KEY);
		
		if (imagesDboList != null) {
		    ImmutableSet.Builder<Image> images = ImmutableSet.builder();
		    for (DBObject imagesDbo : imagesDboList) {
		        images.add(imageTranslator.fromDBObject(imagesDbo, null));
		    }
		    entity.setImages(images.build());
		}

		entity.setTags(TranslatorUtils.toSet(dbObject, TAGS_KEY));
		entity.setThumbnail((String) dbObject.get(THUMBNAIL_KEY));
		entity.setTitle((String) dbObject.get(TITLE_KEY));

		String cType = (String) dbObject.get(MEDIA_TYPE_KEY);
		if (cType != null) {
			entity.setMediaType(MediaType.valueOf(cType.toUpperCase()));
		}

		String specialization = (String) dbObject.get(SPECIALIZATION_KEY);
		if (specialization != null) {
			entity.setSpecialization(Specialization.valueOf(specialization.toUpperCase()));
		}
		
		entity.setPresentationChannel(TranslatorUtils.toString(dbObject, PRESENTATION_CHANNEL_KEY));
		
		if (dbObject.containsField(ACTIVELY_PUBLISHED_KEY)) {
		    entity.setActivelyPublished(TranslatorUtils.toBoolean(dbObject, ACTIVELY_PUBLISHED_KEY));
		}

		return entity;
	}
	
    @SuppressWarnings("unchecked")
    private void decodeRelatedLinks(DBObject dbObject, Described entity) {
        if (dbObject.containsField(LINKS_KEY)) {
            entity.setRelatedLinks(Iterables.transform((Iterable<DBObject>) dbObject.get(LINKS_KEY), new Function<DBObject, RelatedLink>() {

                @Override
                public RelatedLink apply(DBObject input) {
                    return relatedLinkTranslator.fromDBObject(input);
                }
            }));
        }
    }

    @Override
	public DBObject toDBObject(DBObject dbObject, Described entity) {
		 if (dbObject == null) {
            dbObject = new BasicDBObject();
         }
		 
	    descriptionTranslator.toDBObject(dbObject, entity);

        encodeRelatedLinks(dbObject, entity);

	    TranslatorUtils.from(dbObject, SHORT_DESC_KEY, entity.getShortDescription());
	    TranslatorUtils.from(dbObject, MEDIUM_DESC_KEY, entity.getMediumDescription());
	    TranslatorUtils.from(dbObject, LONG_DESC_KEY, entity.getLongDescription());

        TranslatorUtils.from(dbObject, DESCRIPTION_KEY, entity.getDescription());
        TranslatorUtils.fromDateTime(dbObject, FIRST_SEEN_KEY, entity.getFirstSeen());
        TranslatorUtils.fromDateTime(dbObject, THIS_OR_CHILD_LAST_UPDATED_KEY, entity.getThisOrChildLastUpdated());
        TranslatorUtils.fromSet(dbObject, entity.getGenres(), GENRES_KEY);
        TranslatorUtils.from(dbObject, IMAGE_KEY, entity.getImage());
        TranslatorUtils.fromDateTime(dbObject, LAST_FETCHED_KEY, entity.getLastFetched());
        
        if (entity.getPublisher() != null) {
        	TranslatorUtils.from(dbObject, PUBLISHER_KEY, entity.getPublisher().key());
        }
        
        TranslatorUtils.fromSet(dbObject, entity.getTags(), TAGS_KEY);
        TranslatorUtils.from(dbObject, THUMBNAIL_KEY, entity.getThumbnail());
        TranslatorUtils.from(dbObject, TITLE_KEY, entity.getTitle());
        
        if (entity.isScheduleOnly()) {
            dbObject.put(SCHEDULE_ONLY_KEY, Boolean.TRUE);
        }
        
        if (entity.getMediaType() != null) {
        	TranslatorUtils.from(dbObject, MEDIA_TYPE_KEY, entity.getMediaType().toString().toLowerCase());
        }
        if (entity.getSpecialization() != null) {
            TranslatorUtils.from(dbObject, SPECIALIZATION_KEY, entity.getSpecialization().toString().toLowerCase());
        }
        
        BasicDBList images = new BasicDBList();
        if (entity.getImages() != null) {
            for (Image image : entity.getImages()) {
                images.add(imageTranslator.toDBObject(null, image));
            }
        }
        dbObject.put(IMAGES_KEY, images);
        
        TranslatorUtils.from(dbObject, PRESENTATION_CHANNEL_KEY, entity.getPresentationChannel());
        TranslatorUtils.from(dbObject, ACTIVELY_PUBLISHED_KEY, entity.isActivelyPublished());
        
        return dbObject;
	}
    
    private void encodeRelatedLinks(DBObject dbObject, Described entity) {
        if (!entity.getRelatedLinks().isEmpty()) {
            BasicDBList values = new BasicDBList(); 
            for(RelatedLink link : entity.getRelatedLinks()) {
                values.add(relatedLinkTranslator.toDBObject(link));
            }
            dbObject.put(LINKS_KEY, values);
        }
    }
	
	static Identified newModel(DBObject dbObject) {
		EntityType type = EntityType.from((String) dbObject.get(TYPE_KEY));
		try {
			return type.getModelClass().newInstance();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

    public void removeFieldsForHash(DBObject dbObject) {
        descriptionTranslator.removeFieldsForHash(dbObject);
        dbObject.removeField(LAST_FETCHED_KEY);
        dbObject.removeField(THIS_OR_CHILD_LAST_UPDATED_KEY);
    }
}
