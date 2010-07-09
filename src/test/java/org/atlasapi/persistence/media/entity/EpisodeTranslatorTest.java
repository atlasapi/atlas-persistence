package org.atlasapi.persistence.media.entity;

import junit.framework.TestCase;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Publisher;

import com.mongodb.DBObject;

public class EpisodeTranslatorTest extends TestCase {

    EpisodeTranslator et = new EpisodeTranslator();
    
    public void testFromEpisode() throws Exception {
        Episode episode = new Episode("canonicalUri", "episodeCurie", Publisher.BBC);
        episode.setEpisodeNumber(1);
        
        DBObject dbo = et.toDBObject(null, episode);
        assertEquals("canonicalUri", dbo.get(DescriptionTranslator.CANONICAL_URI));
        assertEquals(1, dbo.get("episodeNumber"));
    }
    
    public void testToEpisode() throws Exception {
        Episode episode = new Episode("canonicalUri", "episodeCurie", Publisher.BBC);
        episode.setEpisodeNumber(1);
        episode.setSeriesNumber(1);
        
        Brand brand = new Brand("uri", "curie", Publisher.BBC);
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
