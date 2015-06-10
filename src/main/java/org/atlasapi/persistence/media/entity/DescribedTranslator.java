package org.atlasapi.persistence.media.entity;

import java.util.Comparator;
import java.util.List;
import java.util.Locale;

import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.EntityType;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Image;
import org.atlasapi.media.entity.LocalizedDescription;
import org.atlasapi.media.entity.LocalizedTitle;
import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Rating;
import org.atlasapi.media.entity.RelatedLink;
import org.atlasapi.media.entity.Review;
import org.atlasapi.media.entity.Specialization;
import org.atlasapi.persistence.ModelTranslator;

import com.google.common.base.Function;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class DescribedTranslator implements ModelTranslator<Described> {

    private static final String TAGS_KEY = "tags";
    private static final String THUMBNAIL_KEY = "thumbnail";
    private static final String TITLE_KEY = "title";
    public static final String MEDIA_TYPE_KEY = "mediaType";
    private static final String SPECIALIZATION_KEY = "specialization";
    private static final String PRESENTATION_CHANNEL_KEY = "presentationChannel";
    public static final String IMAGES_KEY = "images";
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
    public static final String ACTIVELY_PUBLISHED_KEY = "activelyPublished";
    public static final String LINKS_KEY = "links";
    protected static final String LOCALIZED_DESCRIPTIONS_KEY = "descriptions";
    protected static final String LOCALIZED_TITLES_KEY = "titles";
    protected static final String REVIEWS_KEY = "reviews";
    protected static final String RATINGS_KEY = "ratings";
    protected static final String AUDIENCE_STATISTICS_KEY = "audienceStatistics";
    protected static final String PRIORITY_KEY = "ratings";
    
    public static final Ordering<LocalizedDescription> LOCALIZED_DESCRIPTION_ORDERING =
            Ordering.from(new Comparator<LocalizedDescription>() {

                @Override
                public int compare(LocalizedDescription o1, LocalizedDescription o2) {
                    return ComparisonChain.start()
                            .compare(o1.getLanguageTag(),
                                    o2.getLanguageTag(),
                                    Ordering.natural().nullsFirst())
                            .compare(o1.getDescription(),
                                    o2.getDescription(),
                                    Ordering.natural().nullsFirst())
                            .compare(o1.getLongDescription(),
                                    o2.getLongDescription(),
                                    Ordering.natural().nullsFirst())
                            .compare(o1.getMediumDescription(),
                                    o2.getMediumDescription(),
                                    Ordering.natural().nullsFirst())
                            .compare(o1.getShortDescription(),
                                    o2.getShortDescription(),
                                    Ordering.natural().nullsFirst())
                            .result();
                }
            });
    
    private static final Ordering<Review> REVIEW_ORDERING = 
            Ordering.from(new Comparator<Review>() {

                @Override
                public int compare(Review o1, Review o2) {
                    return ComparisonChain.start()
                            .compare(localeToLanguageTag(o1.getLocale()), localeToLanguageTag(o2.getLocale()), Ordering.natural().nullsFirst())
                            .compare(o1.getReview(), o2.getReview(), Ordering.natural().nullsFirst())
                            .result();
                }
            });
    
    private static final String localeToLanguageTag(Locale locale) {
        if (locale == null) {
            return null;
        }
        return locale.toLanguageTag();
    }

    public static final Ordering<LocalizedTitle> LOCALIZED_TITLE_ORDERING =
            Ordering.from(new Comparator<LocalizedTitle>() {

                @Override
                public int compare(LocalizedTitle o1, LocalizedTitle o2) {
                    return ComparisonChain.start()
                            .compare(o1.getLanguageTag(),
                                    o2.getLanguageTag(),
                                    Ordering.natural().nullsFirst())
                            .compare(o1.getTitle(), o2.getTitle(), Ordering.natural().nullsFirst())
                            .result();
                }
            });

	
	private final IdentifiedTranslator identifiedTranslator;
    private final ImageTranslator imageTranslator;
    private final RelatedLinkTranslator relatedLinkTranslator;
    private final LocalizedDescriptionTranslator localizedDescriptionTranslator;
    private final LocalizedTitleTranslator localizedTitleTranslator;
    private final ReviewTranslator reviewTranslator;
    private final AudienceStatisticsTranslator audienceStatisticsTranslator;
    private final RatingsTranslator ratingsTranslator;

	public DescribedTranslator(IdentifiedTranslator identifiedTranslator, ImageTranslator imageTranslator) {
		this.identifiedTranslator = identifiedTranslator;
		this.imageTranslator = imageTranslator;
        this.relatedLinkTranslator = new RelatedLinkTranslator();
        this.localizedDescriptionTranslator = new LocalizedDescriptionTranslator();
        this.localizedTitleTranslator = new LocalizedTitleTranslator();
        this.reviewTranslator = new ReviewTranslator();
        this.ratingsTranslator = new RatingsTranslator();
        this.audienceStatisticsTranslator = new AudienceStatisticsTranslator();
	}
	
	@Override
	public Described fromDBObject(DBObject dbObject, Described entity) {

		identifiedTranslator.fromDBObject(dbObject, entity);

        decodeRelatedLinks(dbObject, entity);
        // this is needed in order to translate some Segments which have Object in the description field.
        Object description = dbObject.get(DESCRIPTION_KEY);
        if(description instanceof DBObject) {
            entity.setDescription(TranslatorUtils.toString((DBObject)description, TITLE_KEY));
        } else {
            entity.setDescription((String) description);
        }

		entity.setShortDescription(TranslatorUtils.toString(dbObject, SHORT_DESC_KEY));
		entity.setMediumDescription(TranslatorUtils.toString(dbObject, MEDIUM_DESC_KEY));
		entity.setLongDescription(TranslatorUtils.toString(dbObject, LONG_DESC_KEY));
		entity.setPriority(TranslatorUtils.toDouble(dbObject, PRIORITY_KEY));

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
		
		if (dbObject.containsField(AUDIENCE_STATISTICS_KEY)) {
		    entity.setAudienceStatistics(audienceStatisticsTranslator.fromDBObject((DBObject)dbObject.get(AUDIENCE_STATISTICS_KEY)));
		}
		
        decodeLocalizedDescriptions(dbObject, entity);
        decodeLocalizedTitles(dbObject, entity);
        decodeReviews(dbObject, entity);
        decodeRatings(dbObject, entity, entity.getPublisher());

		return entity;
	}
	
    private void decodeLocalizedTitles(DBObject dbObject, Described entity) {
        List<DBObject> localisedTitlesDBO = TranslatorUtils.toDBObjectList(dbObject,
                LOCALIZED_TITLES_KEY);

        entity.setLocalizedTitles(Iterables.transform(localisedTitlesDBO,
                new Function<DBObject, LocalizedTitle>() {

                    @Override
                    public LocalizedTitle apply(DBObject input) {
                        return localizedTitleTranslator.fromDBObject(input, new LocalizedTitle());
                    }
                }));
    }

    private void decodeLocalizedDescriptions(DBObject dbObject, Described entity) {
        List<DBObject> localisedDescriptionsDBO = TranslatorUtils.toDBObjectList(dbObject,
                LOCALIZED_DESCRIPTIONS_KEY);

        entity.setLocalizedDescriptions(Iterables.transform(localisedDescriptionsDBO,
                new Function<DBObject, LocalizedDescription>() {

                    @Override
                    public LocalizedDescription apply(DBObject input) {
                        return localizedDescriptionTranslator.fromDBObject(input,
                                new LocalizedDescription());
                    }
                }));
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

    private void decodeReviews(DBObject dbObject, Described entity) {
        entity.setReviews(TranslatorUtils.toIterable(dbObject, REVIEWS_KEY, new Function<DBObject, Review>() {

            @Override
            public Review apply(DBObject dbo) {
                return reviewTranslator.fromDBObject(dbo);
            }
            
        }).or(ImmutableSet.<Review>of()));
    }
    
    private void decodeRatings(DBObject dbObject, Described entity, final Publisher publisher) {
        entity.setRatings(
                TranslatorUtils.toIterable(dbObject, RATINGS_KEY, ratingsTranslator.fromDbo(publisher))
                .or(ImmutableSet.<Rating>of())
                );
    }
    
    @Override
	public DBObject toDBObject(DBObject dbObject, Described entity) {
		 if (dbObject == null) {
            dbObject = new BasicDBObject();
         }
		 
	    identifiedTranslator.toDBObject(dbObject, entity);

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
        TranslatorUtils.from(dbObject, PRIORITY_KEY, entity.getPriority());
        
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
        
        if (entity.getAudienceStatistics() != null) {
            DBObject audienceDbo = new BasicDBObject();
            audienceStatisticsTranslator.toDBObject(audienceDbo, entity.getAudienceStatistics());
            dbObject.put(AUDIENCE_STATISTICS_KEY, audienceDbo);
        }
        
        encodeLocalizedDescriptions(entity, dbObject);
        encodeLocalizedTitles(entity, dbObject);
        encodeReviews(dbObject, entity);
        encodeRatings(dbObject, entity);
        
        return dbObject;
	}
    
    private void encodeLocalizedTitles(Described entity, DBObject dbObject) {
        List<LocalizedTitle> sortedTitles = LOCALIZED_TITLE_ORDERING.sortedCopy(entity.getLocalizedTitles());
        
        BasicDBList dbTitles = new BasicDBList();
        
        for (LocalizedTitle localizedTitle: sortedTitles) {
            dbTitles.add(localizedTitleTranslator.toDBObject(new BasicDBObject(), localizedTitle));
        }

        dbObject.put(LOCALIZED_TITLES_KEY, dbTitles);
    }

    private void encodeLocalizedDescriptions(Described entity, DBObject dbObject) {
        List<LocalizedDescription> sortedDescriptions = LOCALIZED_DESCRIPTION_ORDERING.sortedCopy(entity.getLocalizedDescriptions());

        BasicDBList dbDescriptions = new BasicDBList();

        for (LocalizedDescription localizedDescriptions : sortedDescriptions) {
            dbDescriptions.add(localizedDescriptionTranslator.toDBObject(new BasicDBObject(),
                    localizedDescriptions));
        }

        dbObject.put(LOCALIZED_DESCRIPTIONS_KEY, dbDescriptions);
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
    
    private void encodeRatings(DBObject dbObject, Described entity) {
        if (!entity.getRatings().isEmpty()) {
            BasicDBList values = new BasicDBList(); 
            for(Rating rating : entity.getRatings()) {
                DBObject dbo = new BasicDBObject();
                values.add(ratingsTranslator.toDBObject(dbo, rating));
            }
            dbObject.put(RATINGS_KEY, values);
        }
    }
    
    private void encodeReviews(DBObject dbObject, Described entity) {
        TranslatorUtils.fromIterable(dbObject, REVIEWS_KEY, REVIEW_ORDERING.sortedCopy(entity.getReviews()), 
                new Function<Review, DBObject>() {

                        @Override
                        public DBObject apply(Review review) {
                            return reviewTranslator.toDBObject(new BasicDBObject(), review);
                        }
            
                });
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
        identifiedTranslator.removeFieldsForHash(dbObject);
        dbObject.removeField(LAST_FETCHED_KEY);
        dbObject.removeField(THIS_OR_CHILD_LAST_UPDATED_KEY);
    }
    
}
