package org.atlasapi.media.channel;

import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.ModelTranslator;
import org.atlasapi.persistence.media.entity.IdentifiedTranslator;

import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class ChannelTranslator implements ModelTranslator<Channel> {

    public static final String TITLE = "title";
	public static final String PUBLISHER = "publisher";
	public static final String BROADCASTER = "broadcaster";
	public static final String MEDIA_TYPE = "mediaType";
	public static final String KEY = "key";

	private ModelTranslator<Identified> identifiedTranslator;

	public ChannelTranslator() {
		this.identifiedTranslator = new IdentifiedTranslator(true);
	}

	@Override
	public DBObject toDBObject(DBObject dbObject, Channel model) {
		dbObject = new BasicDBObject();

		identifiedTranslator.toDBObject(dbObject, model);
		
		TranslatorUtils.from(dbObject, TITLE, model.title());
		TranslatorUtils.from(dbObject, MEDIA_TYPE, model.mediaType().name());
		TranslatorUtils.from(dbObject, PUBLISHER, model.publisher().key());
		TranslatorUtils.from(dbObject, BROADCASTER, model.broadcaster() != null ? model.broadcaster().key() : null);
		TranslatorUtils.from(dbObject, KEY, model.key());
		return dbObject;
	}

	@Override
	public Channel fromDBObject(DBObject dbObject, Channel model) {
	    if (dbObject == null) {
	        return null;
	    }
	    
		if (model == null) {
			model = new Channel();
		}

        model.setPublisher(Publisher.fromKey(TranslatorUtils.toString(dbObject, PUBLISHER)).requireValue());
		model.setMediaType(MediaType.valueOf(TranslatorUtils.toString(dbObject, MEDIA_TYPE)));
		model.setTitle((String) dbObject.get(TITLE));
		model.setKey((String) dbObject.get(KEY));
		
		String broadcaster = TranslatorUtils.toString(dbObject, BROADCASTER);
		model.setBroadcaster(broadcaster != null ? Publisher.fromKey(broadcaster).valueOrNull() : null);
		
		return (Channel) identifiedTranslator.fromDBObject(dbObject, model);
	}

}
