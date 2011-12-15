package org.atlasapi.persistence.content.mongo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Series;
import org.atlasapi.persistence.lookup.NewLookupWriter;
import org.atlasapi.persistence.media.entity.ContainerTranslator;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Test;

import com.google.common.collect.Iterables;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.persistence.MongoTestHelper;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.time.DateTimeZones;
import com.metabroadcast.common.time.SystemClock;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;

public class MongoContentWriterTest {

    private final DatabasedMongo mongo = MongoTestHelper.anEmptyTestDatabase();
    
    private final NewLookupWriter lookupWriter = new NewLookupWriter() {
        @Override
        public void ensureLookup(Content described) {
        }
    };
    
    private final MongoContentWriter contentWriter = new MongoContentWriter(mongo, lookupWriter, new SystemClock());
    
    private final DBCollection children = mongo.collection("children");
    private final DBCollection topLevelItems = mongo.collection("topLevelItems");
    private final DBCollection containers = mongo.collection("containers");
    private final DBCollection programmeGroups = mongo.collection("programmeGroups");
    
    private final ContainerTranslator containerTranslator = new ContainerTranslator(new SubstitutionTableNumberCodec());
    
    @After
    public void clearDb() {
        children.remove(new BasicDBObject());
        topLevelItems.remove(new BasicDBObject());
        containers.remove(new BasicDBObject());
        programmeGroups.remove(new BasicDBObject());
    }
    
    @Test
    public void testCreatingItemWithoutWrittenContainerFails() {

        Item item = new Item("itemUri", "itemCurie", Publisher.BBC);

        Container container = new Container("containerUri", "containerCurie", Publisher.BBC);

        item.setContainer(container);

        try {
            contentWriter.createOrUpdate(item);
            fail("Expected an exception");
        } catch (IllegalStateException e) {

            assertNull(children.findOne(item.getCanonicalUri()));

        }
    }
    
    @Test
    public void testCreatingEpisodeWithoutWrittenBrandFails() {
        
        Episode item = new Episode("itemUri", "itemCurie", Publisher.BBC);
        Series series = new Series("seriesUri","seriesCurie", Publisher.BBC);
        Brand brand = new Brand("brandUri", "brandCurie", Publisher.BBC);
        
        item.setSeries(series);
        item.setContainer(brand);
        
        try {
            contentWriter.createOrUpdate(item);
            fail("Expected an exception");
        } catch (IllegalStateException e) {
            assertNull(children.findOne(item.getCanonicalUri()));
        }
    }
    
    @Test
    public void testCreatingEpisodeWithoutWrittenSeriesFailsAndDoesntChangeBrand() {
        
        Episode item = new Episode("itemUri", "itemCurie", Publisher.BBC);
        item.setThisOrChildLastUpdated(new DateTime(DateTimeZones.UTC));
        
        Series series = new Series("seriesUri","seriesCurie", Publisher.BBC);
        Brand brand = new Brand("brandUri", "brandCurie", Publisher.BBC);
        
        item.setSeries(series);
        item.setContainer(brand);
        
        contentWriter.createOrUpdate(brand);
        
        try {
            contentWriter.createOrUpdate(item);
            fail("Expected an exception");
        } catch (IllegalStateException e) {
            
            Brand retrievedBrand = (Brand) containerTranslator.fromDB(containers.findOne(brand.getCanonicalUri()));
            
            assertTrue(retrievedBrand.getChildRefs().isEmpty());
            
            assertNull(children.findOne(item.getCanonicalUri()));
        }
    }

    @Test
    public void testWritingEpisodeInSeriesInBrand() {
        
        Episode item = new Episode("itemUri", "itemCurie", Publisher.BBC);
        item.setThisOrChildLastUpdated(new DateTime(DateTimeZones.UTC));
        
        Series series = new Series("seriesUri","seriesCurie", Publisher.BBC);
        series.setThisOrChildLastUpdated(new DateTime(DateTimeZones.UTC));
        Brand brand = new Brand("brandUri", "brandCurie", Publisher.BBC);
        
        series.setParent(brand);
        
        item.setSeries(series);
        item.setContainer(brand);
        
        contentWriter.createOrUpdate(brand);
        contentWriter.createOrUpdate(series);
        contentWriter.createOrUpdate(item);
        
        assertNotNull(children.findOne(item.getCanonicalUri()));
        
        Series retrievedSeries = (Series) containerTranslator.fromDB(programmeGroups.findOne(series.getCanonicalUri()));
        assertEquals(item.getCanonicalUri(), Iterables.getOnlyElement(retrievedSeries.getChildRefs()).getUri());
        
        assertNull(containers.findOne(series.getCanonicalUri()));
        
        Brand retrievedBrand = (Brand) containerTranslator.fromDB(containers.findOne(brand.getCanonicalUri()));
        assertEquals(item.getCanonicalUri(), Iterables.getOnlyElement(retrievedBrand.getChildRefs()).getUri());
        assertEquals(series.getCanonicalUri(), Iterables.getOnlyElement(retrievedBrand.getSeriesRefs()).getUri());
    }
    
    @Test
    public void testWritingEpisodeInTopLevelSeries() {
       
        Episode item = new Episode("itemUri", "itemCurie", Publisher.BBC);
        item.setThisOrChildLastUpdated(new DateTime(DateTimeZones.UTC));
        
        Series series = new Series("seriesUri","seriesCurie", Publisher.BBC);
        series.setThisOrChildLastUpdated(new DateTime(DateTimeZones.UTC));
        
        item.setContainer(series);
        item.setSeries(series);
        
        contentWriter.createOrUpdate(series);
        contentWriter.createOrUpdate(item);
        
        assertNotNull(children.findOne(item.getCanonicalUri()));
        
        Series retrievedSeries = (Series) containerTranslator.fromDB(programmeGroups.findOne(series.getCanonicalUri()));
        assertEquals(item.getCanonicalUri(), Iterables.getOnlyElement(retrievedSeries.getChildRefs()).getUri());
        
        assertNotNull(containers.findOne(series.getCanonicalUri()));
        
    }
    
    
    @Test
    public void testWritingContainer() {
        
        Container container = new Container("containerUri", "containerCurie", Publisher.BBC);
        
        contentWriter.createOrUpdate(container);
        
        assertNotNull(containers.findOne(container.getCanonicalUri()));
    }
    
    @Test
    public void testConvertingBetweenEpisodeAndTopLevelItem() {
        
        Item item = new Item("itemUri", "itemCurie", Publisher.BBC);
        
        contentWriter.createOrUpdate(item);
        
        assertNotNull(topLevelItems.findOne(item.getCanonicalUri()));
        
        Episode episode = new Episode("itemUri", "itemCurie", Publisher.BBC);
        Brand brand = new Brand("brandUri", "brandUri", Publisher.BBC);
        
        episode.setContainer(brand);
        
        contentWriter.createOrUpdate(brand);
        contentWriter.createOrUpdate(episode);
        
        assertNull(topLevelItems.findOne(item.getCanonicalUri()));
        assertNotNull(children.findOne(item.getCanonicalUri()));
        
    }
}
