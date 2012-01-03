package org.atlasapi.persistence.media.entity;

import org.atlasapi.media.entity.Channel;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.ModelTranslator;

import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class ChannelTranslator implements ModelTranslator<Channel> {

	public static final String TITLE = "title";
	public static final String PUBLISHER = "publisher";
	public static final String MEDIA_TYPE = "mediaType";
	public static final String KEY = "key";

	private ModelTranslator<Identified> identifiedTranslator;

	public ChannelTranslator() {
		this.identifiedTranslator = new DescriptionTranslator(true);
	}

	@Override
	public DBObject toDBObject(DBObject dbObject, Channel model) {
		dbObject = new BasicDBObject();

		identifiedTranslator.toDBObject(dbObject, model);
		
		TranslatorUtils.from(dbObject, TITLE, model.title());
		TranslatorUtils.from(dbObject, MEDIA_TYPE, model.mediaType().name());
		TranslatorUtils.from(dbObject, PUBLISHER, model.publisher().key());
		TranslatorUtils.from(dbObject, KEY, model.key());
		return dbObject;
	}

	@Override
	public Channel fromDBObject(DBObject dbObject, Channel model) {
		if (model == null) {
			model = new Channel();
		}

		model.setPublisher(Publisher.fromKey((String) dbObject.get(PUBLISHER))
				.requireValue());
		model.setMediaType(MediaType.valueOf((String) dbObject.get(MEDIA_TYPE)));
		model.setTitle((String) dbObject.get(TITLE));
		model.setKey((String) dbObject.get(KEY));
		return (Channel) identifiedTranslator.fromDBObject(dbObject, model);
	}

}
