package org.atlasapi.media.channel;

import static com.google.common.collect.Iterables.transform;

import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Image;
import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.RelatedLink;
import org.atlasapi.persistence.ModelTranslator;
import org.atlasapi.persistence.media.entity.IdentifiedTranslator;
import org.atlasapi.persistence.media.entity.RelatedLinkTranslator;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class ChannelTranslator implements ModelTranslator<Channel> {

    public static final String TITLE = "title";
    public static final String TITLES = "titles";
	public static final String PUBLISHER = "publisher";
	public static final String BROADCASTER = "broadcaster";
	public static final String MEDIA_TYPE = "mediaType";
	public static final String AVAILABLE_ON = "availableOn";
	public static final String HIGH_DEFINITION = "highDefinition";
	public static final String REGIONAL = "regional";
	public static final String ADULT = "adult";
	public static final String TIMESHIFT = "timeshift";
	public static final String KEY = "key";
	public static final String IMAGE = "image";
	public static final String IMAGES = "images";
	public static final String NEW_IMAGES = "new_images";
	public static final String RELATED_LINKS = "relatedLinks";
	public static final String PARENT = "parent";
	public static final String VARIATIONS = "variations";
	public static final String NUMBERINGS = "numberings";
	public static final String START_DATE = "startDate";
	public static final String END_DATE = "endDate";
	public static final String GENRES_KEY = "genres";
	

	private ModelTranslator<Identified> identifiedTranslator;
	private ChannelNumberingTranslator channelNumberingTranslator;
	private TemporalTitleTranslator temporalTitleTranslator;
	private final TemporalImageTranslator temporalImageTranslator;
	private RelatedLinkTranslator relatedLinkTranslator;

	public ChannelTranslator() {
		this.identifiedTranslator = new IdentifiedTranslator(true);
		this.channelNumberingTranslator = new ChannelNumberingTranslator();
		this.temporalTitleTranslator = new TemporalTitleTranslator();
		this.temporalImageTranslator = new TemporalImageTranslator();
		this.relatedLinkTranslator = new RelatedLinkTranslator();
	}

	@Override
	public DBObject toDBObject(DBObject dbObject, Channel model) {
		dbObject = new BasicDBObject();

		identifiedTranslator.toDBObject(dbObject, model);
		
		temporalTitleTranslator.fromTemporalTitleSet(dbObject, TITLES, model.getAllTitles());
		temporalImageTranslator.fromTemporalImageSet(dbObject, NEW_IMAGES, model.getAllImages());
		// TODO remove this once migration is complete
		setPreviousOldChannelImagesField(dbObject, IMAGES, model.getAllImages());
		
		encodeRelatedLinks(dbObject, model);
		
		TranslatorUtils.from(dbObject, MEDIA_TYPE, model.getMediaType().name());
		TranslatorUtils.from(dbObject, PUBLISHER, model.getSource().key());
		TranslatorUtils.from(dbObject, HIGH_DEFINITION, model.getHighDefinition());
		TranslatorUtils.from(dbObject, REGIONAL, model.getRegional());
		TranslatorUtils.from(dbObject, ADULT, model.getAdult());
		TranslatorUtils.fromDuration(dbObject, TIMESHIFT, model.getTimeshift());
		TranslatorUtils.from(dbObject, BROADCASTER, model.getBroadcaster() != null ? model.getBroadcaster().key() : null);
		if (model.getAvailableFrom() != null) {
		    TranslatorUtils.fromSet(dbObject, ImmutableSet.copyOf(transform(model.getAvailableFrom(), Publisher.TO_KEY)), AVAILABLE_ON);
		}
		TranslatorUtils.from(dbObject, KEY, model.getKey());
		TranslatorUtils.from(dbObject, PARENT, model.getParent());
		if (model.getVariations() != null) {
		    TranslatorUtils.fromLongSet(dbObject, VARIATIONS, model.getVariations());
		}
		if (model.getChannelNumbers() != null) {
		    channelNumberingTranslator.fromChannelNumberingSet(dbObject, NUMBERINGS, model.getChannelNumbers());
		}
		TranslatorUtils.fromLocalDate(dbObject, START_DATE, model.getStartDate());
		TranslatorUtils.fromLocalDate(dbObject, END_DATE, model.getEndDate());
		TranslatorUtils.fromSet(dbObject, model.getGenres(), GENRES_KEY);
		
		return dbObject;
	}

    @Override
	public Channel fromDBObject(DBObject dbObject, Channel model) {
	    if (dbObject == null) {
	        return null;
	    }
	    
		if (model == null) {
			model = Channel.builder().build();
		}

        model.setSource(Publisher.fromKey(TranslatorUtils.toString(dbObject, PUBLISHER)).requireValue());
		model.setMediaType(MediaType.valueOf(TranslatorUtils.toString(dbObject, MEDIA_TYPE)));
        if (dbObject.containsField(TITLES)) {
            model.setTitles(temporalTitleTranslator.toTemporalTitleSet(dbObject, TITLES));
        }
        if (dbObject.containsField(TITLE)) {
            model.addTitle(TranslatorUtils.toString(dbObject, TITLE));
        }
        if (dbObject.containsField(NEW_IMAGES)) {
            model.setImages(temporalImageTranslator.toTemporalImageSet(dbObject, NEW_IMAGES));
        }
        
        decodeRelatedLinks(dbObject, model);
		model.setKey((String) dbObject.get(KEY));
		model.setHighDefinition(TranslatorUtils.toBoolean(dbObject, HIGH_DEFINITION));
		model.setRegional(TranslatorUtils.toBoolean(dbObject, REGIONAL));
		model.setAdult(TranslatorUtils.toBoolean(dbObject, ADULT));
		model.setTimeshift(TranslatorUtils.toDuration(dbObject, TIMESHIFT));
		model.setAvailableFrom(Iterables.transform(TranslatorUtils.toSet(dbObject, AVAILABLE_ON), Publisher.FROM_KEY));
		
		String broadcaster = TranslatorUtils.toString(dbObject, BROADCASTER);
		model.setBroadcaster(broadcaster != null ? Publisher.fromKey(broadcaster).valueOrNull() : null);
		model.setParent(TranslatorUtils.toLong(dbObject, PARENT));
		model.setVariationIds(TranslatorUtils.toLongSet(dbObject, VARIATIONS));
		model.setChannelNumbers(channelNumberingTranslator.toChannelNumberingSet(dbObject, NUMBERINGS));
		model.setStartDate(TranslatorUtils.toLocalDate(dbObject, START_DATE));
		model.setEndDate(TranslatorUtils.toLocalDate(dbObject, END_DATE));
		if (dbObject.containsField(GENRES_KEY)) {
		    model.setGenres(TranslatorUtils.toSet(dbObject, GENRES_KEY));
		}
		
		return (Channel) identifiedTranslator.fromDBObject(dbObject, model);
	}

    @SuppressWarnings("unchecked")
    private void decodeRelatedLinks(DBObject dbObject, Channel channel) {
        if (dbObject.containsField(RELATED_LINKS)) {
            channel.setRelatedLinks(Iterables.transform(
                (Iterable<DBObject>) dbObject.get(RELATED_LINKS), 
                new Function<DBObject, RelatedLink>() {
                    @Override
                    public RelatedLink apply(DBObject input) {
                        return relatedLinkTranslator.fromDBObject(input);
                    }
                }
            ));
        }
    }

    private void encodeRelatedLinks(DBObject dbObject, Channel channel) {
        if (!channel.getRelatedLinks().isEmpty()) {
            BasicDBList values = new BasicDBList(); 
            for(RelatedLink link : channel.getRelatedLinks()) {
                values.add(relatedLinkTranslator.toDBObject(link));
            }
            dbObject.put(RELATED_LINKS, values);
        }
    }

    // Translates the new images set into the previous set of images, for migration purposes
    // uses the title translator, as they have the same form, and use the same keys
    // to determine which are the images which were used previously, we use the IS_PRIMARY_IMAGE filter,
    // which returns images of theme Light_Opaque
    private void setPreviousOldChannelImagesField(DBObject dbObject, String key, Iterable<TemporalField<Image>> newImages) {
        Iterable<TemporalField<String>> primaryImages = Iterables.transform(
            Iterables.filter(
                newImages, 
                new Predicate<TemporalField<Image>>() {
                    @Override
                    public boolean apply(TemporalField<Image> input) {
                        Image image = input.getValue();
                        return Channel.IS_PRIMARY_IMAGE.apply(image);
                    }
                }
            ),
            new Function<TemporalField<Image>, TemporalField<String>>() {
                @Override
                public TemporalField<String> apply(TemporalField<Image> input) {
                    return new TemporalField<String>(
                        input.getValue().getCanonicalUri(), 
                        input.getStartDate(), 
                        input.getEndDate()
                    );
                }
            
        });
        
        BasicDBList values = new BasicDBList();
        for (TemporalField<String> value : primaryImages) {
            if (value != null) {
                values.add(temporalTitleTranslator.toDBObject(value));
            }
        }
        dbObject.put(key, values);
    }
}
