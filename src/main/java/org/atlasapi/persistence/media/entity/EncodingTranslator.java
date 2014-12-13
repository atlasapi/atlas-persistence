package org.atlasapi.persistence.media.entity;

import java.util.List;
import java.util.Set;

import org.atlasapi.media.entity.Encoding;
import org.atlasapi.media.entity.Location;
import org.atlasapi.persistence.ModelTranslator;

import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.metabroadcast.common.media.MimeType;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBList;
import com.mongodb.DBObject;

public class EncodingTranslator implements ModelTranslator<Encoding> {
	
    static final String LOCATIONS_KEY = "availableAt";
    private static final String AUDIO_DESCRIBED_KEY = "audioDescribed";
    private static final String SIGNED_KEY = "signed";

    private final IdentifiedTranslator descriptionTranslator = new IdentifiedTranslator();
    private final LocationTranslator locationTranslator = new LocationTranslator();

    @SuppressWarnings("unchecked")
    @Override
    public Encoding fromDBObject(DBObject dbObject, Encoding entity) {
        if (entity == null) {
            entity = new Encoding();
        }
        
        descriptionTranslator.fromDBObject(dbObject, entity);
        
        entity.setAdvertisingDuration((Integer) dbObject.get("advertisingDuration"));
        entity.setAudioBitRate((Integer) dbObject.get("audioBitRate"));
        entity.setAudioChannels((Integer) dbObject.get("audioChannels"));
        
        entity.setAudioCoding(readAudioCoding(dbObject));
        
        entity.setBitRate((Integer) dbObject.get("bitRate"));
        entity.setAudioChannels((Integer) dbObject.get("audioChannels"));
        entity.setContainsAdvertising((Boolean) dbObject.get("containsAdvertising"));
        entity.setDataContainerFormat(readContainerFormat(dbObject));
        entity.setDataSize((Long) dbObject.get("dataSize"));
        entity.setDistributor((String) dbObject.get("distributor"));
        entity.setHasDOG((Boolean) dbObject.get("hasDOG"));
        entity.setSource((String) dbObject.get("source"));
        entity.setVideoAspectRatio((String) dbObject.get("videoAspectRatio"));
        entity.setVideoBitRate((Integer) dbObject.get("videoBitRate"));
        
        entity.setVideoCoding(readVideoCoding(dbObject));
        
        entity.setVideoFrameRate(TranslatorUtils.toFloat(dbObject, "videoFrameRate"));
        
        entity.setVideoHorizontalSize((Integer) dbObject.get("videoHorizontalSize"));
        entity.setVideoProgressiveScan((Boolean) dbObject.get("videoProgressiveScan"));
        entity.setVideoVerticalSize((Integer) dbObject.get("videoVerticalSize"));

        if (dbObject.get(AUDIO_DESCRIBED_KEY) != null) {
            entity.setAudioDescribed((Boolean) dbObject.get(AUDIO_DESCRIBED_KEY));
        }

        if (dbObject.get(SIGNED_KEY) != null) {
            entity.setSigned((Boolean) dbObject.get(SIGNED_KEY));
        }
        
        List<DBObject> list = (List<DBObject>) dbObject.get(LOCATIONS_KEY);
        if (list != null && ! list.isEmpty()) {
            Set<Location> locations = Sets.newHashSet();
            for (DBObject object: list) {
                Location location = locationTranslator.fromDBObject(object, null);
                locations.add(location);
            }
            entity.setAvailableAt(locations);
        }
        
        return entity;
    }

    private MimeType readVideoCoding(DBObject dbObject) {
    	String codingAsString = (String) dbObject.get("videoCoding");
    	if (codingAsString == null) {
    		return null;
    	}
    	return MimeType.fromString(codingAsString);
    }
    
    private MimeType readAudioCoding(DBObject dbObject) {
    	String codingAsString = (String) dbObject.get("audioCoding");
    	if (codingAsString == null) {
    		return null;
    	}
    	return MimeType.fromString(codingAsString);
    }

	private MimeType readContainerFormat(DBObject dbObject) {
    	String formatName = (String) dbObject.get("dataContainerFormat");
    	if (formatName == null) {
    		return null;
    	}
    	return MimeType.fromString(formatName);
    }

	@Override
    public DBObject toDBObject(DBObject dbObject, Encoding entity) {
        dbObject = descriptionTranslator.toDBObject(dbObject, entity);
        
        TranslatorUtils.from(dbObject, "advertisingDuration", entity.getAdvertisingDuration());
        TranslatorUtils.from(dbObject, "audioBitRate", entity.getAudioBitRate());
        TranslatorUtils.from(dbObject, "audioChannels", entity.getAudioChannels());
        
        if (entity.getAudioCoding() != null) {
        	TranslatorUtils.from(dbObject, "audioCoding", entity.getAudioCoding().toString());
        }
        
        TranslatorUtils.from(dbObject, "bitRate", entity.getBitRate());
        TranslatorUtils.from(dbObject, "audioChannels", entity.getAudioChannels());
        TranslatorUtils.from(dbObject, "containsAdvertising", entity.getContainsAdvertising());
        
        if (entity.getDataContainerFormat() != null) {
        	TranslatorUtils.from(dbObject, "dataContainerFormat", entity.getDataContainerFormat().toString());
        }
        
        TranslatorUtils.from(dbObject, "dataSize", entity.getDataSize());
        TranslatorUtils.from(dbObject, "distributor", entity.getDistributor());
        TranslatorUtils.from(dbObject, "hasDOG", entity.getHasDOG());
        TranslatorUtils.from(dbObject, "source", entity.getSource());
        TranslatorUtils.from(dbObject, "videoAspectRatio", entity.getVideoAspectRatio());
        TranslatorUtils.from(dbObject, "videoBitRate", entity.getVideoBitRate());
       
        if (entity.getVideoCoding() != null) {
        	TranslatorUtils.from(dbObject, "videoCoding", entity.getVideoCoding().toString());
        }
        
        TranslatorUtils.from(dbObject, "videoFrameRate", entity.getVideoFrameRate());
        TranslatorUtils.from(dbObject, "videoHorizontalSize", entity.getVideoHorizontalSize());
        TranslatorUtils.from(dbObject, "videoProgressiveScan", entity.getVideoProgressiveScan());
        TranslatorUtils.from(dbObject, "videoVerticalSize", entity.getVideoVerticalSize());
        TranslatorUtils.from(dbObject, AUDIO_DESCRIBED_KEY, entity.getAudioDescribed());
        TranslatorUtils.from(dbObject, SIGNED_KEY, entity.getSigned());

        if (! entity.getAvailableAt().isEmpty()) {
            BasicDBList list = new BasicDBList();
            for (Location location: LOCATION_ORDERING.immutableSortedCopy(entity.getAvailableAt())) {
                list.add(locationTranslator.toDBObject(null, location));
            }
            dbObject.put(LOCATIONS_KEY, list);
        }
        
        return dbObject;
    }
	
    private static final Ordering<Location> LOCATION_ORDERING = new Ordering<Location>() {

        @Override
        public int compare(Location left, Location right) {
            ComparisonChain chain = ComparisonChain.start()
                    .compare(left.getCanonicalUri(), right.getCanonicalUri(), Ordering.natural().nullsLast())
                    .compare(left.getUri(), right.getUri(), Ordering.natural().nullsLast());
            
            if (left.getPolicy() != null && right.getPolicy() != null) {
                chain.compare(left.getPolicy().getPlatform(), right.getPolicy().getPlatform(), Ordering.natural().nullsLast())
                     .compare(left.getPolicy().getNetwork(), right.getPolicy().getNetwork(), Ordering.natural().nullsLast())
                     .compare(left.getPolicy().getService(), right.getPolicy().getService(), Ordering.natural().nullsLast())
                     .compare(left.getPolicy().getPlayer(), right.getPolicy().getPlayer(), Ordering.natural().nullsLast())
                     .compare(left.getPolicy().getAvailabilityStart(), right.getPolicy().getAvailabilityStart(), Ordering.natural().nullsLast());
            }
            return chain.result();
        }
        
    };

}
