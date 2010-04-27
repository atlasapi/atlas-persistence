package org.uriplay.persistence.media.entity;

import java.util.List;
import java.util.Set;

import org.uriplay.media.entity.Broadcast;
import org.uriplay.media.entity.Encoding;
import org.uriplay.media.entity.Version;

import com.google.common.collect.Sets;
import com.mongodb.BasicDBList;
import com.mongodb.DBObject;

public class VersionTranslator implements DBObjectEntityTranslator<Version> {
    private final DescriptionTranslator descriptionTranslator;
    private final BroadcastTranslator broadcastTranslator;
    private final EncodingTranslator encodingTranslator;
    
    public VersionTranslator(DescriptionTranslator descriptionTranslator, BroadcastTranslator broadcastTranslator, EncodingTranslator encodingTranslator) {
        this.descriptionTranslator = descriptionTranslator;
        this.broadcastTranslator = broadcastTranslator;
        this.encodingTranslator = encodingTranslator;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Version fromDBObject(DBObject dbObject, Version entity) {
        if (entity == null) {
            entity = new Version();
        }
        
        descriptionTranslator.fromDBObject(dbObject, entity);
        
        entity.setDuration((Integer) dbObject.get("duration"));
        entity.setPublishedDuration((Integer) dbObject.get("publishedDuration"));
        entity.setRating((String) dbObject.get("rating"));
        entity.setRatingText((String) dbObject.get("ratingText"));
        
        List<DBObject> list = (List) dbObject.get("broadcasts");
        if (list != null && ! list.isEmpty()) {
            Set<Broadcast> broadcasts = Sets.newHashSet();
            for (DBObject object: list) {
                Broadcast broadcast = broadcastTranslator.fromDBObject(object, null);
                broadcasts.add(broadcast);
            }
            entity.setBroadcasts(broadcasts);
        }
        
        list = (List) dbObject.get("manifestedAs");
        if (list != null && ! list.isEmpty()) {
            Set<Encoding> encodings = Sets.newHashSet();
            for (DBObject object: list) {
                Encoding encoding = encodingTranslator.fromDBObject(object, null);
                encodings.add(encoding);
            }
            entity.setManifestedAs(encodings);
        }
        
        return entity;
    }

    @Override
    public DBObject toDBObject(DBObject dbObject, Version entity) {
        dbObject = descriptionTranslator.toDBObject(dbObject, entity);
        
        TranslatorUtils.from(dbObject, "duration", entity.getDuration());
        TranslatorUtils.from(dbObject, "publishedDuration", entity.getPublishedDuration());
        TranslatorUtils.from(dbObject, "rating", entity.getRating());
        TranslatorUtils.from(dbObject, "ratingText", entity.getRatingText());
        
        if (! entity.getBroadcasts().isEmpty()) {
            BasicDBList list = new BasicDBList();
            for (Broadcast broadcast: entity.getBroadcasts()) {
                list.add(broadcastTranslator.toDBObject(null, broadcast));
            }
            dbObject.put("broadcasts", list);
        }
        
        if (! entity.getManifestedAs().isEmpty()) {
            BasicDBList list = new BasicDBList();
            for (Encoding encoding: entity.getManifestedAs()) {
                list.add(encodingTranslator.toDBObject(null, encoding));
            }
            dbObject.put("manifestedAs", list);
        }
        
        return dbObject;
    }

}
