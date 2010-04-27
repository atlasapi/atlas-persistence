package org.uriplay.persistence.media.entity;

import junit.framework.TestCase;

import org.uriplay.media.entity.Brand;
import org.uriplay.media.entity.Episode;

import com.mongodb.DBObject;

public class EpisodeTranslatorTest extends TestCase {
    DescriptionTranslator dt = new DescriptionTranslator();
    BroadcastTranslator brt = new BroadcastTranslator(dt);
    LocationTranslator lt = new LocationTranslator(dt);
    EncodingTranslator ent = new EncodingTranslator(dt, lt);
    VersionTranslator vt = new VersionTranslator(dt, brt, ent);
    ItemTranslator it = new ItemTranslator(dt, vt);
    PlaylistTranslator pt = new PlaylistTranslator(dt);
    BrandTranslator bt = new BrandTranslator(pt);
    EpisodeTranslator et = new EpisodeTranslator(it, bt);
    
    public void testFromEpisode() throws Exception {
        Episode episode = new Episode();
        episode.setCanonicalUri("canonicalUri");
        episode.setEpisodeNumber(1);
        
        DBObject dbo = et.toDBObject(null, episode);
        assertEquals("canonicalUri", dbo.get("canonicalUri"));
        assertEquals(1, dbo.get("episodeNumber"));
    }
    
    public void testToEpisode() throws Exception {
        Episode episode = new Episode();
        episode.setCanonicalUri("canonicalUri");
        episode.setEpisodeNumber(1);
        episode.setSeriesNumber(1);
        
        Brand brand = new Brand();
        brand.setCanonicalUri("uri");
        brand.setCurie("curie");
        episode.setBrand(brand);
        
        DBObject dbo = et.toDBObject(null, episode);
        
        Episode e = et.fromDBObject(dbo, null);
        assertEquals(e.getCanonicalUri(), episode.getCanonicalUri());
        assertEquals(e.getEpisodeNumber(), episode.getEpisodeNumber());
        assertEquals(e.getSeriesNumber(), episode.getSeriesNumber());
        Brand b = e.getBrand();
        assertNotNull(b);
        assertEquals(brand.getCanonicalUri(), b.getCanonicalUri());
        assertEquals(brand.getCurie(), b.getCurie());
    }
}
