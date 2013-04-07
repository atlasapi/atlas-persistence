package org.atlasapi.persistence.media.entity;

import java.util.List;
import java.util.Set;

import junit.framework.TestCase;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.content.Container;
import org.atlasapi.media.entity.EntityType;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Series;

import com.google.common.collect.Sets;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.time.SystemClock;
import com.mongodb.DBObject;

public class ContainerTranslatorTest extends TestCase {
    
	ContainerTranslator bt = new ContainerTranslator(new SubstitutionTableNumberCodec());
    
    @SuppressWarnings("unchecked")
    public void testFromBrand() throws Exception {
        Brand brand = new Brand("canonicalUri", "curie", Publisher.BBC);
        brand.setFirstSeen(new SystemClock().now());
        
        Set<String> genres = Sets.newHashSet();
        genres.add("genre");
        brand.setGenres(genres);
        
        DBObject obj = bt.toDBObject(null, brand);
        assertEquals("canonicalUri", obj.get(IdentifiedTranslator.ID));
        
        List<String> i = (List<String>) obj.get("genres");
        assertEquals(1, i.size());
        for (String genre: i) {
            brand.getGenres().contains(genre);
        }
    }
    
    public void testToBrand() throws Exception {
        Brand brand = new Brand("canonicalUri", "curie", Publisher.BBC);
        brand.setId(1);
        brand.setFirstSeen(new SystemClock().now());
        brand.setLastFetched(new SystemClock().now());
        
        Set<String> genres = Sets.newHashSet();
        genres.add("genres");
        brand.setGenres(genres);
        
        DBObject obj = bt.toDBObject(null, brand);
        
        Container b = bt.fromDBObject(obj, null);
        assertEquals(brand.getCanonicalUri(), b.getCanonicalUri());
        assertEquals(brand.getFirstSeen(), b.getFirstSeen());
        assertEquals(brand.getLastFetched(), b.getLastFetched());
        
        Set<String> g = brand.getGenres();
        assertEquals(1, g.size());
        for (String genre: g) {
            assertTrue(genres.contains(genre));
        }
    }
    
    public void testEncodeDecodeSeries() {
        
        Series series = new Series("testUri", "testCurie", Publisher.BBC);
        series.setId(1);
        series.setParentRef(new ParentRef(2L, EntityType.BRAND));
        series.withSeriesNumber(5);
        series.setTotalEpisodes(5);
        
        DBObject encoded = bt.toDB(series);
        
        Series fromDBObject = (Series) bt.fromDBObject(encoded, null);
        
        assertEquals(series.getParent(), fromDBObject.getParent());
        assertEquals(series.getSeriesNumber(), fromDBObject.getSeriesNumber());
        assertEquals(series.getTotalEpisodes(), fromDBObject.getTotalEpisodes());
        
    }
}
