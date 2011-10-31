package org.atlasapi.persistence.media.entity;

import java.util.List;

import org.atlasapi.media.entity.Clip;
import org.atlasapi.media.entity.Content;
import org.atlasapi.persistence.ModelTranslator;

import com.google.common.collect.Lists;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class ContentTranslator implements ModelTranslator<Content> {

	public static String CLIPS_KEY = "clips";
	public static String TOPICS_KEY = "topics";
	public static String ID_KEY = "id";
	
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
			List<Clip> clips = Lists.newArrayList();
			for (DBObject dbo : clipsDbos) {
                clips.add(clipTranslator.fromDBObject(dbo, null));
            }
			entity.setClips(clips);
		}
		
		if (dbObject.containsField(TOPICS_KEY)) {
		    entity.setTopicUris(TranslatorUtils.toSet(dbObject, TOPICS_KEY));
		}
		
		entity.setId(TranslatorUtils.toString(dbObject, ID_KEY));

		return entity;
	}

	@Override
	public DBObject toDBObject(DBObject dbObject, Content entity) {
		dbObject = describedTranslator.toDBObject(dbObject, entity);
		if (!entity.getClips().isEmpty()) {
		    BasicDBList clipDbos = new BasicDBList();
		    for (Clip clip : entity.getClips()) {
		        clipDbos.add(clipTranslator.toDBObject(new BasicDBObject(), clip));
            }
			dbObject.put(CLIPS_KEY, clipDbos);
		}
		
		if (!entity.getTopics().isEmpty()) {
		    BasicDBList topics = new BasicDBList();
		    topics.addAll(entity.getTopics());
		    dbObject.put(TOPICS_KEY, topics);
		}
		
		TranslatorUtils.from(dbObject, ID_KEY, entity.getId());
		
        return dbObject;
	}
}
