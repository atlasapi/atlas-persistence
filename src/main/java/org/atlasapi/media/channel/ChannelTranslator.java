package org.atlasapi.media.channel;

import static com.google.common.collect.Iterables.transform;

import java.util.List;
import java.util.Set;

import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.ModelTranslator;
import org.atlasapi.persistence.media.entity.IdentifiedTranslator;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class ChannelTranslator implements ModelTranslator<Channel> {

    public static final String TITLE = "title";
	public static final String PUBLISHER = "publisher";
	public static final String BROADCASTER = "broadcaster";
	public static final String MEDIA_TYPE = "mediaType";
	public static final String AVAILABLE_ON = "availableOn";
	public static final String HIGH_DEFINITION = "highDefinition";
	public static final String KEY = "key";
	public static final String IMAGE = "image";
	public static final String PARENT = "parent";
	public static final String VARIATIONS = "variations";
	public static final String NUMBERINGS = "numberings";

	private ModelTranslator<Identified> identifiedTranslator;
	private ModelTranslator<ChannelNumbering> channelNumberingTranslator;

	public ChannelTranslator() {
		this.identifiedTranslator = new IdentifiedTranslator(true);
		this.channelNumberingTranslator = new ChannelNumberingTranslator();
	}

	@Override
	public DBObject toDBObject(DBObject dbObject, Channel model) {
		dbObject = new BasicDBObject();

		identifiedTranslator.toDBObject(dbObject, model);
		
		TranslatorUtils.from(dbObject, TITLE, model.title());
		TranslatorUtils.from(dbObject, MEDIA_TYPE, model.mediaType().name());
		TranslatorUtils.from(dbObject, PUBLISHER, model.source().key());
		TranslatorUtils.from(dbObject, HIGH_DEFINITION, model.highDefinition());
		TranslatorUtils.from(dbObject, BROADCASTER, model.broadcaster() != null ? model.broadcaster().key() : null);
		if (model.availableFrom() != null) {
		    TranslatorUtils.fromSet(dbObject, ImmutableSet.copyOf(transform(model.availableFrom(), Publisher.TO_KEY)), AVAILABLE_ON);
		}
		TranslatorUtils.from(dbObject, KEY, model.key());
		TranslatorUtils.from(dbObject, IMAGE, model.image());
		TranslatorUtils.from(dbObject, PARENT, model.parent());
		if (model.variations() != null) {
		    TranslatorUtils.fromLongSet(dbObject, VARIATIONS, model.variations());
		}
		
		fromChannelNumberingSet(dbObject, NUMBERINGS, model.channelNumbers());
		
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
		model.setTitle((String) dbObject.get(TITLE));
		model.setKey((String) dbObject.get(KEY));
		model.setHighDefinition((Boolean) dbObject.get(HIGH_DEFINITION));
		model.setAvailableFrom(Iterables.transform(TranslatorUtils.toSet(dbObject, AVAILABLE_ON), Publisher.FROM_KEY));
		model.setImage(TranslatorUtils.toString(dbObject, IMAGE));
		
		String broadcaster = TranslatorUtils.toString(dbObject, BROADCASTER);
		model.setBroadcaster(broadcaster != null ? Publisher.fromKey(broadcaster).valueOrNull() : null);
		model.setParent(TranslatorUtils.toLong(dbObject, PARENT));
		model.setVariationIds(TranslatorUtils.toLongSet(dbObject, VARIATIONS));
		model.setChannelNumbers(toChannelNumberingSet(dbObject, NUMBERINGS));
		
		return (Channel) identifiedTranslator.fromDBObject(dbObject, model);
	}
	
	private void fromChannelNumberingSet(DBObject dbObject, String key, Set<ChannelNumbering> set) {
	    BasicDBList values = new BasicDBList();
        for (ChannelNumbering value : set) {
            if (value != null) {
                values.add(channelNumberingTranslator.toDBObject(null, value));
            }
        }
        dbObject.put(key, values);
	}
	
	@SuppressWarnings("unchecked")
    private Set<ChannelNumbering> toChannelNumberingSet(DBObject object, String name) {
        if (object.containsField(name)) {
            List<ChannelNumbering> dbValues = (List<ChannelNumbering>) channelNumberingTranslator.fromDBObject(object, null);
            return Sets.newLinkedHashSet(dbValues);
        }
        return Sets.newLinkedHashSet();
    }
}
