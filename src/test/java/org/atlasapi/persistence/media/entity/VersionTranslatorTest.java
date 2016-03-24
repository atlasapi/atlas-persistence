package org.atlasapi.persistence.media.entity;

import junit.framework.TestCase;

import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Encoding;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Restriction;
import org.atlasapi.media.entity.Version;
import org.joda.time.Duration;

import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.time.Clock;
import com.metabroadcast.common.time.SystemClock;
import com.mongodb.DBObject;

public class VersionTranslatorTest extends TestCase {
	
	private final Clock clock = new SystemClock();
	
    VersionTranslator vt = new VersionTranslator(new SubstitutionTableNumberCodec());
    
    public void testFromVersion() throws Exception {
        Version version = new Version();
        version.setDuration(Duration.standardSeconds(2));
        version.setProvider(Publisher.C4);
        
        DBObject dbObject = vt.toDBObject(null, version);
        
        assertEquals(version.getDuration(), dbObject.get("duration"));
        assertEquals(version.getProvider().key(), dbObject.get("provider"));
    }
    
    public void testToVersion() throws Exception {
        Version version = new Version();
        version.setDuration(Duration.standardSeconds(2));
        version.setPublishedDuration(1);
        version.setProvider(Publisher.BBC);
        
        Broadcast broadcast = new Broadcast("channel", clock.now(), Duration.standardSeconds(1));
        broadcast.setCanonicalUri("uri");
        version.addBroadcast(broadcast);
        
        Encoding encoding = new Encoding();
        encoding.setAudioChannels(1);
        encoding.setBitRate(2);
        version.addManifestedAs(encoding);
        
        Restriction restriction = new Restriction();
        restriction.setRestricted(true);
        restriction.setMinimumAge(15);
        restriction.setMessage("text");
        version.setRestriction(restriction);
        
        DBObject dbObject = vt.toDBObject(null, version);
        
        Version v = vt.fromDBObject(dbObject, null, true);
        assertEquals(version.getDuration(), v.getDuration());
        assertEquals(version.getPublishedDuration(), v.getPublishedDuration());
        assertEquals(version.getProvider(), v.getProvider());
        
        Broadcast b = v.getBroadcasts().iterator().next();
        assertEquals(broadcast.getBroadcastDuration(), b.getBroadcastDuration());
        
        Encoding e = v.getManifestedAs().iterator().next();
        assertEquals(encoding.getAudioChannels(), e.getAudioChannels());
        assertEquals(encoding.getBitRate(), e.getBitRate());
        
        Restriction r = v.getRestriction();
        assertEquals(restriction.isRestricted(), r.isRestricted());
        assertEquals(restriction.getMinimumAge(), r.getMinimumAge());
        assertEquals(restriction.getMessage(), r.getMessage());
    }
}
