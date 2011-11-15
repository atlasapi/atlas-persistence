package org.atlasapi.persistence.media.entity;

import java.util.List;

import org.atlasapi.media.entity.Clip;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.KeyPhrase;
import org.atlasapi.persistence.ModelTranslator;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class ContentTranslator implements ModelTranslator<Content> {

    private static final String PHRASES_KEY = "phrases";
    public static final String CLIPS_KEY = "clips";
	
	private final ClipTranslator clipTranslator;
	private final KeyPhraseTranslator keyPhraseTranslator;
	private final DescribedTranslator describedTranslator;
	
	public ContentTranslator() {
		this(new DescribedTranslator(new DescriptionTranslator()), new ClipTranslator());
	}
	
	//TODO: why not use collaborators interface here? ModelTranslator<Described> etc...
	public ContentTranslator(DescribedTranslator describedTranslator, ClipTranslator clipTranslator) {
		this.describedTranslator = describedTranslator;
		this.clipTranslator = clipTranslator;
		this.keyPhraseTranslator = new KeyPhraseTranslator();
	}
	
	@Override
	public Content fromDBObject(DBObject dbObject, Content entity) {
		describedTranslator.fromDBObject(dbObject, entity);

		decodeClips(dbObject, entity);
        decodeKeyPhrases(dbObject, entity);

		return entity;
	}

	@SuppressWarnings("unchecked")
    private void decodeKeyPhrases(DBObject dbObject, Content entity) {
        if (dbObject.containsField(PHRASES_KEY)) {
            entity.setKeyPhrases(Iterables.transform((Iterable<DBObject>) dbObject.get(PHRASES_KEY), new Function<DBObject, KeyPhrase>() {
                @Override
                public KeyPhrase apply(DBObject input) {
                    return keyPhraseTranslator.fromDBObject(input);
                }
            }));
        }
    }

	@SuppressWarnings("unchecked")
    private void decodeClips(DBObject dbObject, Content entity) {
        if (dbObject.containsField(CLIPS_KEY)) {
			Iterable<DBObject> clipsDbos = (Iterable<DBObject>) dbObject.get(CLIPS_KEY);
			List<Clip> clips = Lists.newArrayList();
			for (DBObject dbo : clipsDbos) {
                clips.add(clipTranslator.fromDBObject(dbo, null));
            }
			entity.setClips(clips);
		}
    }

	@Override
	public DBObject toDBObject(DBObject dbObject, Content entity) {
		dbObject = describedTranslator.toDBObject(dbObject, entity);
		encodeClips(dbObject, entity);
        encodeKeyPhrases(dbObject, entity);
        return dbObject;
	}

    private void encodeClips(DBObject dbObject, Content entity) {
        if (!entity.getClips().isEmpty()) {
		    BasicDBList clipDbos = new BasicDBList();
		    for (Clip clip : entity.getClips()) {
		        clipDbos.add(clipTranslator.toDBObject(new BasicDBObject(), clip));
            }
			dbObject.put(CLIPS_KEY, clipDbos);
		}
    }

    private void encodeKeyPhrases(DBObject dbObject, Content entity) {
        if (!entity.getKeyPhrases().isEmpty()) {
            dbObject.put(PHRASES_KEY, ImmutableSet.copyOf(Iterables.transform(entity.getKeyPhrases(), new Function<KeyPhrase, DBObject>() {
                @Override
                public DBObject apply(KeyPhrase input) {
                    return keyPhraseTranslator.toDBObject(input);
                }
            })));
        }
    }
}
