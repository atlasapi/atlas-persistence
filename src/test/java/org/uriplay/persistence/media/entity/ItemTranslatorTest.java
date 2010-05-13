package org.uriplay.persistence.media.entity;

import java.util.List;
import java.util.Set;

import junit.framework.TestCase;

import org.joda.time.LocalDate;
import org.uriplay.media.entity.Broadcast;
import org.uriplay.media.entity.Encoding;
import org.uriplay.media.entity.Item;
import org.uriplay.media.entity.Location;
import org.uriplay.media.entity.Playlist;
import org.uriplay.media.entity.Version;

import com.google.common.collect.Sets;
import com.mongodb.BasicDBList;
import com.mongodb.DBObject;

public class ItemTranslatorTest extends TestCase {
    DescriptionTranslator dt = new DescriptionTranslator();
    BroadcastTranslator brt = new BroadcastTranslator(dt);
    LocationTranslator lt = new LocationTranslator(dt, new PolicyTranslator());
    EncodingTranslator ent = new EncodingTranslator(dt, lt);
    VersionTranslator vt = new VersionTranslator(dt, brt, ent);
    ItemTranslator it = new ItemTranslator(dt, vt);
    PlaylistTranslator pt = new PlaylistTranslator(dt);
    BrandTranslator bt = new BrandTranslator(pt);
    EpisodeTranslator et = new EpisodeTranslator(it, bt);
    
    @SuppressWarnings("unchecked")
    public void testConvertFromItem() throws Exception {
        Item item = new Item();
        item.setCanonicalUri("canonicalUri");
        item.setTitle("title");
        
        Location loc = new Location();
        loc.setAvailable(true);
        
        Encoding enc = new Encoding();
        enc.setAdvertisingDuration(1);
        enc.addAvailableAt(loc);
        
        Broadcast br = new Broadcast();
        br.setScheduleDate(new LocalDate(2010, 3, 20));
        
        Version version = new Version();
        version.setDuration(1);
        version.addManifestedAs(enc);
        version.addBroadcast(br);
        item.addVersion(version);
        
        Set<String> tags = Sets.newHashSet();
        tags.add("tag");
        item.setTags(tags);
        
        ItemTranslator it = new ItemTranslator(dt, vt);
        DBObject dbObject = it.toDBObject(null, item);
        
        assertEquals("canonicalUri", dbObject.get(DescriptionTranslator.CANONICAL_URI));
        assertEquals("title", dbObject.get("title"));
        
        List<String> t = (List<String>) dbObject.get("tags");
        assertFalse(t.isEmpty());
        for (String tag: t) {
            assertTrue(tags.contains(tag));
        }
        
        BasicDBList vs = (BasicDBList) dbObject.get("versions");
        assertEquals(1, vs.size());
        DBObject v = (DBObject) vs.get(0);
        assertEquals(version.getDuration(), v.get("duration"));
        
        BasicDBList bs = (BasicDBList) v.get("broadcasts");
        assertEquals(1, bs.size());
        DBObject b = (DBObject) bs.get(0);
        assertEquals(br.getScheduleDate().toString(), b.get("scheduleDate"));
        
        BasicDBList ma = (BasicDBList) v.get("manifestedAs");
        assertEquals(1, ma.size());
        DBObject e = (DBObject) ma.get(0);
        assertEquals(enc.getAdvertisingDuration(), e.get("advertisingDuration"));
        
        BasicDBList ls = (BasicDBList) e.get("availableAt");
        assertEquals(1, ls.size());
        DBObject l = (DBObject) ls.get(0);
        assertEquals(loc.getAvailable(), l.get("available"));
    }
    
    public void testConvertToItem() throws Exception {
        Item item = new Item();
        item.setCanonicalUri("canonicalUri");
        item.setTitle("title");
        
        Playlist playlist = new Playlist();
        Set<Playlist> playlists = Sets.newHashSet(playlist);
        item.setContainedIn(playlists);
        playlist.setCanonicalUri("uri");
        
        Location loc = new Location();
        loc.setAvailable(true);
        
        Encoding enc = new Encoding();
        enc.setAdvertisingDuration(1);
        enc.addAvailableAt(loc);
        
        Broadcast br = new Broadcast();
        br.setScheduleDate(new LocalDate(2010, 3, 20));
        
        Version version = new Version();
        version.setDuration(1);
        version.addManifestedAs(enc);
        version.addBroadcast(br);
        item.addVersion(version);
        
        Set<String> tags = Sets.newHashSet();
        tags.add("tag");
        item.setTags(tags);
        
        ItemTranslator it = new ItemTranslator(dt, vt);
        DBObject dbObject = it.toDBObject(null, item);
        
        Item i = it.fromDBObject(dbObject, null);
        assertEquals(i.getCanonicalUri(), item.getCanonicalUri());
        assertEquals(i.getCurie(), item.getCurie());
        Set<String> cUris = i.getContainedInUris();
        assertEquals(1, cUris.size());
        assertEquals(playlist.getCanonicalUri(), cUris.iterator().next());
        
        Set<String> t = i.getTags();
        for (String tag: t) {
            assertTrue(item.getTags().contains(tag));
        }
        
        Set<Version> vs = item.getVersions();
        assertEquals(1, vs.size());
        Version v = vs.iterator().next();
        assertEquals(version.getDuration(), v.getDuration());
        
        Set<Broadcast> bs = v.getBroadcasts();
        assertEquals(1, bs.size());
        Broadcast b = bs.iterator().next();
        assertEquals(br.getScheduleDate(), b.getScheduleDate());
        
        Set<Encoding> ma = v.getManifestedAs();
        assertEquals(1, ma.size());
        Encoding e = ma.iterator().next();
        assertEquals(enc.getAdvertisingDuration(), e.getAdvertisingDuration());
        
        Set<Location> ls = e.getAvailableAt();
        assertEquals(1, ls.size());
        assertEquals(loc.getAvailable(), ls.iterator().next().getAvailable());
    }
}
