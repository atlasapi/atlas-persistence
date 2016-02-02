package org.atlasapi.persistence.media.entity;

import org.atlasapi.media.entity.Encoding;
import org.atlasapi.media.entity.Location;
import org.atlasapi.media.entity.Quality;

import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.media.MimeType;
import com.metabroadcast.common.persistence.MongoTestHelper;
import com.metabroadcast.common.persistence.mongo.MongoConstants;
import com.metabroadcast.common.persistence.mongo.MongoQueryBuilder;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

import junit.framework.TestCase;

public class EncodingTranslatorTest extends TestCase {

	private final EncodingTranslator ent = new EncodingTranslator();
    
    public void testFromEncoding() throws Exception {
        Encoding encoding = new Encoding();
        encoding.setAudioBitRate(1);
        encoding.setContainsAdvertising(true);
        encoding.setVideoFrameRate(1.0F);
        encoding.setAudioDescribed(true);
        encoding.setSigned(true);
        encoding.setSubtitled(true);
        encoding.setQuality(Quality.SD);
        encoding.setQualityDetail("quality_detail");
        
        DBObject dbObject = ent.toDBObject(null, encoding);
        
        assertEquals(encoding.getAudioBitRate(), dbObject.get("audioBitRate"));
        assertEquals(encoding.getContainsAdvertising(), dbObject.get("containsAdvertising"));
        assertEquals(encoding.getVideoFrameRate(), dbObject.get("videoFrameRate"));
        assertEquals(encoding.getAudioDescribed(), dbObject.get("audioDescribed"));
        assertEquals(encoding.getSigned(), dbObject.get("signed"));
        assertEquals(encoding.getSubtitled(), dbObject.get("subtitled"));
        assertEquals(encoding.getQuality().toString().toLowerCase(), dbObject.get("quality"));
        assertEquals(encoding.getQualityDetail(), dbObject.get("qualityDetail"));
    }
    
    public void testToEncoding() throws Exception {
        
        MongoTestHelper.ensureMongoIsRunning();
        DBCollection collection = MongoTestHelper.anEmptyTestDatabase().collection("test");
        
        Encoding encoding = new Encoding();
        encoding.setAudioBitRate(1);
        encoding.setContainsAdvertising(true);
        encoding.setVideoFrameRate(1.0F);
        encoding.setAdvertisingDuration(1);
        encoding.setAudioChannels(1);
        encoding.setAudioCoding(MimeType.AUDIO_3GPP);
        encoding.setBitRate(1);
        encoding.setDataContainerFormat(MimeType.VIDEO_H264);
        encoding.setDataSize(1L);
        encoding.setDistributor("dis");
        encoding.setHasDOG(true);
        encoding.setSource("source");
        encoding.setVideoAspectRatio("ar");
        encoding.setVideoBitRate(2);
        encoding.setVideoCoding(MimeType.VIDEO_H263);
        encoding.setVideoFrameRate(2.0F);
        encoding.setVideoHorizontalSize(1);
        encoding.setVideoProgressiveScan(true);
        encoding.setVideoVerticalSize(2);
        encoding.setAudioDescribed(true);
        encoding.setSigned(true);
        encoding.setSubtitled(false);
        encoding.setHighDefinition(true);
        encoding.setQuality(Quality.SD);
        Location location = new Location();
        location.setCanonicalUri("uri");
        location.setUri("uri");
        location.setVat(123.3);
        location.setSubtitledLanguages(ImmutableSet.of("english"));
        location.setRequiredEncryption(true);
        encoding.addAvailableAt(location);
        
        DBObject dbObject = ent.toDBObject(null, encoding);
        
        dbObject.put(MongoConstants.ID, "test");
        
        collection.save(dbObject);
        
        Encoding enc = ent.fromDBObject(collection.findOne(new MongoQueryBuilder().idEquals("test").build()), null);
        
        assertEquals(encoding.getAudioBitRate(), enc.getAudioBitRate());
        assertEquals(encoding.getContainsAdvertising(), enc.getContainsAdvertising());
        assertEquals(encoding.getVideoFrameRate(), enc.getVideoFrameRate());
        assertEquals(encoding.getAdvertisingDuration(), enc.getAdvertisingDuration());
        assertEquals(encoding.getAudioChannels(), enc.getAudioChannels());
        assertEquals(encoding.getAudioCoding(), enc.getAudioCoding());
        assertEquals(encoding.getBitRate(), enc.getBitRate());
        assertEquals(encoding.getDataContainerFormat(), enc.getDataContainerFormat());
        assertEquals(encoding.getDataSize(), enc.getDataSize());
        assertEquals(encoding.getDistributor(), enc.getDistributor());
        assertEquals(encoding.getHasDOG(), enc.getHasDOG());
        assertEquals(encoding.getSource(), enc.getSource());
        assertEquals(encoding.getVideoAspectRatio(), enc.getVideoAspectRatio());
        assertEquals(encoding.getVideoBitRate(), enc.getVideoBitRate());
        assertEquals(encoding.getVideoCoding(), enc.getVideoCoding());
        assertEquals(encoding.getVideoFrameRate(), enc.getVideoFrameRate());
        assertEquals(encoding.getVideoHorizontalSize(), enc.getVideoHorizontalSize());
        assertEquals(encoding.getVideoProgressiveScan(), enc.getVideoProgressiveScan());
        assertEquals(encoding.getVideoVerticalSize(), enc.getVideoVerticalSize());
        assertEquals(encoding.getAudioDescribed(), enc.getAudioDescribed());
        assertEquals(encoding.getSigned(), enc.getSigned());
        assertEquals(encoding.getSubtitled(), enc.getSubtitled());
        assertEquals(encoding.getHighDefinition(), enc.getHighDefinition());
        assertEquals(encoding.getQuality(), enc.getQuality());

        Location l = enc.getAvailableAt().iterator().next();
        assertEquals(location.getCanonicalUri(), l.getCanonicalUri());
        assertEquals(location.getUri(), l.getUri());
        assertEquals(location.getVat(), l.getVat());
        assertEquals(location.getSubtitledLanguages(), l.getSubtitledLanguages());
        assertEquals(location.getRequiredEncryption(), l.getRequiredEncryption());
    }
}
