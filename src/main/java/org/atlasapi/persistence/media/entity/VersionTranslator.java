package org.atlasapi.persistence.media.entity;

import java.util.List;
import java.util.Set;

import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Encoding;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Restriction;
import org.atlasapi.media.entity.Version;
import org.atlasapi.persistence.ModelTranslator;
import org.joda.time.Duration;

import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBList;
import com.mongodb.DBObject;

public class VersionTranslator implements ModelTranslator<Version> {
    
	static final String ENCODINGS_KEY = "manifestedAs";
    static final String BROADCASTS_KEY = "broadcasts";
    private static final String PROVIDER_KEY = "provider";
	
	private final DescriptionTranslator descriptionTranslator = new DescriptionTranslator();
    private final BroadcastTranslator broadcastTranslator = new BroadcastTranslator();
    private final EncodingTranslator encodingTranslator = new EncodingTranslator();
    private final RestrictionTranslator restrictionTranslator = new RestrictionTranslator();

    @Override
    public Version fromDBObject(DBObject dbObject, Version entity) {
        if (entity == null) {
            entity = new Version();
        }
        
        descriptionTranslator.fromDBObject(dbObject, entity);
        Integer durationInSeconds = (Integer) dbObject.get("duration");
		if (durationInSeconds != null) {
			entity.setDuration(Duration.standardSeconds(durationInSeconds));
		}
		
		if (dbObject.containsField(PROVIDER_KEY)) {
			entity.setProvider(Publisher.fromKey((String) dbObject.get(PROVIDER_KEY)).requireValue());
		}
		
        entity.setPublishedDuration((Integer) dbObject.get("publishedDuration"));

        if(dbObject.get("restriction") != null) {
        	entity.setRestriction(restrictionTranslator.fromDBObject((DBObject) dbObject.get("restriction"), null));
        } else {
        	entity.setRestriction(new Restriction());
        }
        
        List<DBObject> list = TranslatorUtils.toDBObjectList(dbObject,BROADCASTS_KEY);
        if (list != null && ! list.isEmpty()) {
            Set<Broadcast> broadcasts = Sets.newLinkedHashSet();
            for (DBObject object: list) {
                Broadcast broadcast = broadcastTranslator.fromDBObject(object);
                broadcasts.add(broadcast);
            }
            entity.setBroadcasts(broadcasts);
        }
        
        list = TranslatorUtils.toDBObjectList(dbObject,ENCODINGS_KEY);
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

        if(entity.getRestriction() != null && entity.getRestriction().hasRestrictionInformation()) {
        	dbObject.put("restriction", restrictionTranslator.toDBObject(null, entity.getRestriction()));
        }
        
        if (entity.getProvider() != null) {
        	dbObject.put(PROVIDER_KEY, entity.getProvider().key());
        }
        
        if (! entity.getBroadcasts().isEmpty()) {
            BasicDBList list = new BasicDBList();
            for (Broadcast broadcast: sortByBroadcastTime(entity.getBroadcasts())) {
                if (broadcast != null) {
                    list.add(broadcastTranslator.toDBObject(broadcast));
                }
            }
            if (! list.isEmpty()) {
                dbObject.put(BROADCASTS_KEY, list);
            }
        }
        
        if (! entity.getManifestedAs().isEmpty()) {
            BasicDBList list = new BasicDBList();
            for (Encoding encoding: entity.getManifestedAs()) {
                if (encoding != null) {
                    list.add(encodingTranslator.toDBObject(null, encoding));
                }
            }
            if (! list.isEmpty()) {
                dbObject.put(ENCODINGS_KEY, list);
            }
        }
        
        return dbObject;
    }

	private List<Broadcast> sortByBroadcastTime(Set<Broadcast> broadcasts) {
		return MOST_RECENT_FIRST.sortedCopy(broadcasts);
	}
	
	private static final Ordering<Broadcast> MOST_RECENT_FIRST = new Ordering<Broadcast>() {

		@Override
		public int compare(Broadcast a, Broadcast b) {
			int broadcastTimeCmp = b.getTransmissionTime().compareTo(a.getTransmissionTime());
			if (broadcastTimeCmp != 0) {
				 return broadcastTimeCmp;
			}
			if (a.getBroadcastOn() != null && b.getBroadcastOn() != null) {
				int channelCmp = a.getBroadcastOn().compareTo(b.getBroadcastOn());
				if (channelCmp != 0) {
					return channelCmp;
				}
			}
			if (a.getSourceId() != null && b.getSourceId() != null) {
				int idCmp = a.getSourceId().compareTo(b.getSourceId());
				if (idCmp != 0) {
					return idCmp;
				}
			}
			return Ordering.arbitrary().compare(a, b);
		}
	};

}
