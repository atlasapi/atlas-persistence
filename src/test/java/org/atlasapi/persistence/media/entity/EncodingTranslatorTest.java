package org.atlasapi.persistence.media.entity;

import junit.framework.TestCase;

import org.atlasapi.media.entity.Encoding;
import org.atlasapi.media.entity.Location;

import com.metabroadcast.common.media.MimeType;
import com.mongodb.DBObject;

public class EncodingTranslatorTest extends TestCase {

	private final EncodingTranslator ent = new EncodingTranslator();
    
    public void testFromEncoding() throws Exception {
        Encoding encoding = new Encoding();
        encoding.setAudioBitRate(1);
        encoding.setContainsAdvertising(true);
        encoding.setVideoFrameRate(1.0F);
        
        DBObject dbObject = ent.toDBObject(null, encoding);
        
        assertEquals(encoding.getAudioBitRate(), dbObject.get("audioBitRate"));
        assertEquals(encoding.getContainsAdvertising(), dbObject.get("containsAdvertising"));
        assertEquals(encoding.getVideoFrameRate(), dbObject.get("videoFrameRate"));
    }
    
    public void testToEncoding() throws Exception {
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
        
        Location location = new Location();
        location.setCanonicalUri("uri");
        location.setUri("uri");
        encoding.addAvailableAt(location);
        
        DBObject dbObject = ent.toDBObject(null, encoding);
        
        Encoding enc = ent.fromDBObject(dbObject, null);
        
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
        
        Location l = enc.getAvailableAt().iterator().next();
        assertEquals(location.getCanonicalUri(), l.getCanonicalUri());
        assertEquals(location.getUri(), l.getUri());
    }
}
