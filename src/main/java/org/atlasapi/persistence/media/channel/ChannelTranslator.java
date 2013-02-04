package org.atlasapi.persistence.media.channel;

import static com.google.common.collect.Iterables.transform;

import java.util.List;
import java.util.Set;

import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.media.ModelTranslator;
import org.atlasapi.persistence.media.entity.IdentifiedTranslator;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelNumbering;
import org.atlasapi.media.channel.TemporalString;

public class ChannelTranslator implements ModelTranslator<Channel> {

    public static final String TITLE = "title";
    public static final String TITLES = "titles";
	public static final String PUBLISHER = "publisher";
	public static final String BROADCASTER = "broadcaster";
	public static final String MEDIA_TYPE = "mediaType";
	public static final String AVAILABLE_ON = "availableOn";
	public static final String HIGH_DEFINITION = "highDefinition";
	public static final String REGIONAL = "regional";
	public static final String TIMESHIFT = "timeshift";
	public static final String KEY = "key";
	public static final String IMAGE = "image";
	public static final String IMAGES = "images";
	public static final String PARENT = "parent";
	public static final String VARIATIONS = "variations";
	public static final String NUMBERINGS = "numberings";
	public static final String START_DATE = "startDate";
	public static final String END_DATE = "endDate";
	

	private ModelTranslator<Identified> identifiedTranslator;
	private ChannelNumberingTranslator channelNumberingTranslator;
	private TemporalStringTranslator temporalStringTranslator;

	public ChannelTranslator() {
		this.identifiedTranslator = new IdentifiedTranslator(true);
		this.channelNumberingTranslator = new ChannelNumberingTranslator();
		this.temporalStringTranslator = new TemporalStringTranslator();
	}

	@Override
	public DBObject toDBObject(DBObject dbObject, Channel model) {
		dbObject = new BasicDBObject();

		identifiedTranslator.toDBObject(dbObject, model);
		
		fromTemporalStringSet(dbObject, TITLES, model.allTitles());
		fromTemporalStringSet(dbObject, IMAGES, model.allImages());
		
		TranslatorUtils.from(dbObject, MEDIA_TYPE, model.mediaType().name());
		TranslatorUtils.from(dbObject, PUBLISHER, model.source().key());
		TranslatorUtils.from(dbObject, HIGH_DEFINITION, model.highDefinition());
		TranslatorUtils.from(dbObject, REGIONAL, model.regional());
		TranslatorUtils.fromDuration(dbObject, TIMESHIFT, model.timeshift());
		TranslatorUtils.from(dbObject, BROADCASTER, model.broadcaster() != null ? model.broadcaster().key() : null);
		if (model.availableFrom() != null) {
		    TranslatorUtils.fromSet(dbObject, ImmutableSet.copyOf(transform(model.availableFrom(), Publisher.TO_KEY)), AVAILABLE_ON);
		}
		TranslatorUtils.from(dbObject, KEY, model.key());
		TranslatorUtils.from(dbObject, PARENT, model.parent());
		if (model.variations() != null) {
		    TranslatorUtils.fromLongSet(dbObject, VARIATIONS, model.variations());
		}
		if (model.channelNumbers() != null) {
		    fromChannelNumberingSet(dbObject, NUMBERINGS, model.channelNumbers());
		}
		TranslatorUtils.fromLocalDate(dbObject, START_DATE, model.startDate());
		TranslatorUtils.fromLocalDate(dbObject, END_DATE, model.endDate());
		
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
            model.setTitles(toTemporalStringSet(dbObject, TITLES));
        }
        if (dbObject.containsField(TITLE)) {
            model.addTitle(TranslatorUtils.toString(dbObject, TITLE));
        }
        if (dbObject.containsField(IMAGES)) {
            model.setImages(toTemporalStringSet(dbObject, IMAGES));
        }
        if (dbObject.containsField(IMAGE)) {
            model.addImage(TranslatorUtils.toString(dbObject, IMAGE));
        }
		model.setKey((String) dbObject.get(KEY));
		model.setHighDefinition(TranslatorUtils.toBoolean(dbObject, HIGH_DEFINITION));
		model.setRegional(TranslatorUtils.toBoolean(dbObject, REGIONAL));
		model.setTimeshift(TranslatorUtils.toDuration(dbObject, TIMESHIFT));
		model.setAvailableFrom(Iterables.transform(TranslatorUtils.toSet(dbObject, AVAILABLE_ON), Publisher.FROM_KEY));
		
		String broadcaster = TranslatorUtils.toString(dbObject, BROADCASTER);
		model.setBroadcaster(broadcaster != null ? Publisher.fromKey(broadcaster).valueOrNull() : null);
		model.setParent(TranslatorUtils.toLong(dbObject, PARENT));
		model.setVariationIds(TranslatorUtils.toLongSet(dbObject, VARIATIONS));
		model.setChannelNumbers(toChannelNumberingSet(dbObject, NUMBERINGS));
		model.setStartDate(TranslatorUtils.toLocalDate(dbObject, START_DATE));
		model.setEndDate(TranslatorUtils.toLocalDate(dbObject, END_DATE));
		
		return (Channel) identifiedTranslator.fromDBObject(dbObject, model);
	}
	
	private void fromChannelNumberingSet(DBObject dbObject, String key, Set<ChannelNumbering> set) {
	    BasicDBList values = new BasicDBList();
        for (ChannelNumbering value : set) {
            if (value != null) {
                values.add(channelNumberingTranslator.toDBObject(value));
            }
        }
        dbObject.put(key, values);
	}
    
    @SuppressWarnings("unchecked")
    private Set<ChannelNumbering> toChannelNumberingSet(DBObject object, String name) {
        Set<ChannelNumbering> channelNumbers = Sets.newLinkedHashSet();
        if (object.containsField(name)) {
            for (DBObject element : (List<DBObject>) object.get(name)) {
                channelNumbers.add(channelNumberingTranslator.fromDBObject(element));
            }
            return channelNumbers;
        }
        return Sets.newLinkedHashSet();
    }
    
    private void fromTemporalStringSet(DBObject dbObject, String key, Iterable<TemporalString> iterable) {
        BasicDBList values = new BasicDBList();
        for (TemporalString value : iterable) {
            if (value != null) {
                values.add(temporalStringTranslator.toDBObject(value));
            }
        }
        dbObject.put(key, values);
    }
    
    @SuppressWarnings("unchecked")
    private Set<TemporalString> toTemporalStringSet(DBObject object, String name) {
        if (object.containsField(name)) {
            Set<TemporalString> temporalString = Sets.newLinkedHashSet();
            for (DBObject element : (List<DBObject>) object.get(name)) {
                temporalString.add(temporalStringTranslator.fromDBObject(element));
            }
            return temporalString;
        }
        return Sets.newLinkedHashSet();
    }
}
