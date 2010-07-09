package org.atlasapi.persistence.media.entity;

import java.util.List;
import java.util.Set;

import junit.framework.TestCase;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Publisher;

import com.google.common.collect.Sets;
import com.metabroadcast.common.time.SystemClock;
import com.mongodb.DBObject;

public class BrandTranslatorTest extends TestCase {

	BrandTranslator bt = new BrandTranslator();
    
    @SuppressWarnings("unchecked")
    public void testFromBrand() throws Exception {
        Brand brand = new Brand("canonicalUri", "curie", Publisher.BBC);
        brand.setFirstSeen(new SystemClock().now());
        
        Set<String> genres = Sets.newHashSet();
        genres.add("genre");
        brand.setGenres(genres);
        
        DBObject obj = bt.toDBObject(null, brand);
        assertEquals("canonicalUri", obj.get(DescriptionTranslator.CANONICAL_URI));
        
        List<String> i = (List<String>) obj.get("genres");
        assertEquals(1, i.size());
        for (String genre: i) {
            brand.getGenres().contains(genre);
        }
    }
    
    public void testToBrand() throws Exception {
        Brand brand = new Brand("canonicalUri", "curie", Publisher.BBC);
        brand.setFirstSeen(new SystemClock().now());
        brand.setLastFetched(new SystemClock().now());
        
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
