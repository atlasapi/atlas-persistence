package org.atlasapi.persistence.media.entity;

import org.atlasapi.media.entity.Clip;
import org.atlasapi.persistence.ModelTranslator;

import com.mongodb.DBObject;

public class ClipTranslator implements ModelTranslator<Clip> {
   
	private final ItemTranslator itemTranslator;
	
	public ClipTranslator(boolean includeId) {
		itemTranslator = new ItemTranslator(new ContentTranslator(new DescriptionTranslator(includeId), this));
	}
    
    @Override
    public Clip fromDBObject(DBObject dbObject, Clip clip) {
        if (clip == null) {
            clip = new Clip();
        }
        itemTranslator.fromDBObject(dbObject, clip);
        return clip;
    }

    @Override
    public DBObject toDBObject(DBObject dbObject, Clip clip) {
        dbObject = itemTranslator.toDBObject(dbObject, clip);
        
        dbObject.put("type", Clip.class.getSimpleName());
        dbObject.put("clipOfUri", clip.getClipOf().getCanonicalUri());
        
        return dbObject;
    }

}
