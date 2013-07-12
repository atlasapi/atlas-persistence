package org.atlasapi.media.channel;

import java.util.List;
import java.util.Set;

import org.atlasapi.media.entity.Image;
import org.atlasapi.persistence.media.entity.ImageTranslator;

import com.google.common.collect.Sets;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class TemporalImageTranslator {
    private static final String START_DATE_KEY = "startDate";
    private static final String END_DATE_KEY = "endDate";
    private static final String IMAGE_KEY = "value";
    
    private final ImageTranslator imageTranslator = new ImageTranslator();
    
    // TODO extract date translation to common utils class
    public DBObject toDBObject(TemporalField<Image> temporalImage) {
        DBObject dbo = new BasicDBObject();
        
        TranslatorUtils.fromLocalDate(dbo, START_DATE_KEY, temporalImage.getStartDate());
        TranslatorUtils.fromLocalDate(dbo, END_DATE_KEY, temporalImage.getEndDate());
        TranslatorUtils.from(dbo, IMAGE_KEY, imageTranslator.toDBObject(null, temporalImage.getValue()));
        
        return dbo;
    }
    
    public TemporalField<Image> fromDBObject(DBObject dbo) {
        return new TemporalField<Image>(
            imageTranslator.fromDBObject(TranslatorUtils.toDBObject(dbo, IMAGE_KEY), null),
            TranslatorUtils.toLocalDate(dbo, START_DATE_KEY),
            TranslatorUtils.toLocalDate(dbo, END_DATE_KEY));
    }
    
    public void fromTemporalImageSet(DBObject dbObject, String key, Iterable<TemporalField<Image>> images) {
        BasicDBList values = new BasicDBList();
        for (TemporalField<Image> value : images) {
            if (value != null) {
                values.add(toDBObject(value));
            }
        }
        dbObject.put(key, values);
    }
    
    @SuppressWarnings("unchecked")
    public Set<TemporalField<Image>> toTemporalImageSet(DBObject object, String name) {
        if (object.containsField(name)) {
            Set<TemporalField<Image>> temporalImage = Sets.newLinkedHashSet();
            for (DBObject element : (List<DBObject>) object.get(name)) {
                temporalImage.add(fromDBObject(element));
            }
            return temporalImage;
        }
        return Sets.newLinkedHashSet();
    }
}
