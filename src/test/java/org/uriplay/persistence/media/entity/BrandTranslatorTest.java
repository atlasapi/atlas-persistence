package org.uriplay.persistence.media.entity;

import java.util.List;
import java.util.Set;

import junit.framework.TestCase;

import org.joda.time.DateTime;
import org.uriplay.media.entity.Brand;

import com.google.common.collect.Sets;
import com.mongodb.DBObject;

public class BrandTranslatorTest extends TestCase {
    PlaylistTranslator pt = new PlaylistTranslator(new ContentTranslator());
    BrandTranslator bt = new BrandTranslator(pt);
    
    @SuppressWarnings("unchecked")
    public void testFromBrand() throws Exception {
        Brand brand = new Brand("canonicalUri", "curie");
        brand.setFirstSeen(new DateTime());
        
        Set<String> genres = Sets.newHashSet();
        genres.add("genre");
        brand.setGenres(genres);
        
        DBObject obj = bt.toDBObject(null, brand);
        assertEquals("canonicalUri", obj.get(DescriptionTranslator.CANONICAL_URI));
        assertEquals(brand.getFirstSeen().getMillis(), obj.get("firstSeen"));
        
        List<String> i = (List<String>) obj.get("genres");
        assertEquals(1, i.size());
        for (String genre: i) {
            brand.getGenres().contains(genre);
        }
    }
    
    public void testToBrand() throws Exception {
        Brand brand = new Brand("canonicalUri", "curie");
        brand.setFirstSeen(new DateTime());
        brand.setLastFetched(new DateTime());
        
        Set<String> genres = Sets.newHashSet();
        genres.add("genres");
        brand.setGenres(genres);
        
        DBObject obj = bt.toDBObject(null, brand);
        
        Brand b = bt.fromDBObject(obj, null);
        assertEquals(brand.getCanonicalUri(), b.getCanonicalUri());
        assertEquals(brand.getFirstSeen(), b.getFirstSeen());
        assertEquals(brand.getLastFetched(), b.getLastFetched());
        
        Set<String> g = brand.getGenres();
        assertEquals(1, g.size());
        for (String genre: g) {
            assertTrue(genres.contains(genre));
        }
    }
}
