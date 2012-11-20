package org.atlasapi.persistence.media.entity;

import org.atlasapi.media.entity.Clip;
import org.atlasapi.persistence.media.ModelTranslator;

import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.mongodb.DBObject;

public class ClipTranslator implements ModelTranslator<Clip> {
   
	private final ItemTranslator itemTranslator;
	
	public ClipTranslator(NumberToShortStringCodec idCodec) {
		itemTranslator = new ItemTranslator(new ContentTranslator(new DescribedTranslator(new IdentifiedTranslator()), this), idCodec);
	}
    
    @Override
    public Clip fromDBObject(DBObject dbObject, Clip clip) {
        if (clip == null) {
            clip = new Clip();
        }
        itemTranslator.fromDBObject(dbObject, clip);
        clip.setClipOf((String) dbObject.get("clipOfUri"));
        return clip;
    }

    @Override
    public DBObject toDBObject(DBObject dbObject, Clip clip) {
        dbObject = itemTranslator.toDBObject(dbObject, clip);
        
        dbObject.put("type", Clip.class.getSimpleName());
        dbObject.put("clipOfUri", clip.getClipOf());
        
        return dbObject;
    }

    public DBObject removeFieldsForHash(DBObject clipDbo) {
        return itemTranslator.removeFieldsForHash(clipDbo);
    }
}
