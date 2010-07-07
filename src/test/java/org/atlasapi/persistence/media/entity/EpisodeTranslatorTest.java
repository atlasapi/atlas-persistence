package org.atlasapi.persistence.media.entity;

import junit.framework.TestCase;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.persistence.media.entity.BrandTranslator;
import org.atlasapi.persistence.media.entity.BroadcastTranslator;
import org.atlasapi.persistence.media.entity.ContentTranslator;
import org.atlasapi.persistence.media.entity.DescriptionTranslator;
import org.atlasapi.persistence.media.entity.EncodingTranslator;
import org.atlasapi.persistence.media.entity.EpisodeTranslator;
import org.atlasapi.persistence.media.entity.ItemTranslator;
import org.atlasapi.persistence.media.entity.LocationTranslator;
import org.atlasapi.persistence.media.entity.PlaylistTranslator;
import org.atlasapi.persistence.media.entity.PolicyTranslator;
import org.atlasapi.persistence.media.entity.VersionTranslator;

import com.mongodb.DBObject;

public class EpisodeTranslatorTest extends TestCase {
    DescriptionTranslator dt = new DescriptionTranslator();
    ContentTranslator ct = new ContentTranslator();
    BroadcastTranslator brt = new BroadcastTranslator();
    LocationTranslator lt = new LocationTranslator(dt, new PolicyTranslator());
    EncodingTranslator ent = new EncodingTranslator(dt, lt);
    VersionTranslator vt = new VersionTranslator(dt, brt, ent);
    ItemTranslator it = new ItemTranslator(ct, vt);
    PlaylistTranslator pt = new PlaylistTranslator(ct);
    BrandTranslator bt = new BrandTranslator(pt);
    EpisodeTranslator et = new EpisodeTranslator(it, bt);
    
    public void testFromEpisode() throws Exception {
        Episode episode = new Episode("canonicalUri", "episodeCurie");
        episode.setEpisodeNumber(1);
        
        DBObject dbo = et.toDBObject(null, episode);
        assertEquals("canonicalUri", dbo.get(DescriptionTranslator.CANONICAL_URI));
        assertEquals(1, dbo.get("episodeNumber"));
    }
    
    public void testToEpisode() throws Exception {
        Episode episode = new Episode("canonicalUri", "episodeCurie");
        episode.setEpisodeNumber(1);
        episode.setSeriesNumber(1);
        
        Brand brand = new Brand("uri", "curie");
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
