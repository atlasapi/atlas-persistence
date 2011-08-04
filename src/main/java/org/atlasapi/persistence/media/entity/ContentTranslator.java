package org.atlasapi.persistence.media.entity;

import java.util.List;

import org.atlasapi.media.entity.Clip;
import org.atlasapi.media.entity.Content;
import org.atlasapi.persistence.ModelTranslator;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class ContentTranslator implements ModelTranslator<Content> {

	public static String CLIPS_KEY = "clips";
	
	private ClipTranslator clipTranslator;
	private final DescribedTranslator describedTranslator;
	
	public ContentTranslator() {
		this(new DescribedTranslator(new DescriptionTranslator()), new ClipTranslator());
	}
	
	public ContentTranslator(DescribedTranslator describedTranslator, ClipTranslator clipTranslator) {
		this.describedTranslator = describedTranslator;
		this.clipTranslator = clipTranslator;
	}
	
	public void setClipTranslator(ClipTranslator clipTranslator) {
		this.clipTranslator = clipTranslator;
	}
	
	@Override
	@SuppressWarnings("unchecked")
	public Content fromDBObject(DBObject dbObject, Content entity) {
		describedTranslator.fromDBObject(dbObject, entity);

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

		return entity;
	}

	@Override
	public DBObject toDBObject(DBObject dbObject, Content entity) {
		dbObject = describedTranslator.toDBObject(dbObject, entity);
		if (!entity.getClips().isEmpty()) {
			List<DBObject> clipDbos = Lists.transform(entity.getClips(), new Function<Clip, DBObject>() {

				@Override
				public DBObject apply(Clip clip) {
					return clipTranslator.toDBObject(new BasicDBObject(), clip);
				}
			});
			dbObject.put(CLIPS_KEY, clipDbos);
		}
        return dbObject;
	}
}
