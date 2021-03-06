package org.atlasapi.persistence.content.mongo;

import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.ChildRef;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Encoding;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Film;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Location;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.media.entity.Provider;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Rating;
import org.atlasapi.media.entity.RelatedLink;
import org.atlasapi.media.entity.Series;
import org.atlasapi.media.entity.SeriesRef;
import org.atlasapi.media.entity.Version;
import org.atlasapi.persistence.audit.PerHourAndDayMongoPersistenceAuditLog;
import org.atlasapi.persistence.audit.PersistenceAuditLog;
import org.atlasapi.persistence.lookup.NewLookupWriter;
import org.atlasapi.persistence.media.entity.ContainerTranslator;
import org.atlasapi.persistence.media.entity.DescribedTranslator;
import org.atlasapi.persistence.media.entity.IdentifiedTranslator;
import org.atlasapi.persistence.media.entity.ItemTranslator;
import org.atlasapi.persistence.player.PlayerResolver;
import org.atlasapi.persistence.service.ServiceResolver;

import com.google.common.collect.Sets;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.persistence.MongoTestHelper;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.time.DateTimeZones;
import com.metabroadcast.common.time.SystemClock;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;

@RunWith( MockitoJUnitRunner.class )
public class MongoContentWriterTest {

    private static DatabasedMongo mongo;
    
    private final NewLookupWriter lookupWriter = new NewLookupWriter() {
        @Override
        public void ensureLookup(Content described) {
        }
    };
    
    private final ServiceResolver serviceResolver = mock(ServiceResolver.class);
    private final PlayerResolver playerResolver = mock(PlayerResolver.class);

    
    private final PersistenceAuditLog persistenceAuditLog = new PerHourAndDayMongoPersistenceAuditLog(mongo);
    private final MongoContentWriter contentWriter = new MongoContentWriter(mongo, lookupWriter, 
                            persistenceAuditLog, playerResolver, serviceResolver, new SystemClock());
    
    private final DBCollection children = mongo.collection("children");
    private final DBCollection topLevelItems = mongo.collection("topLevelItems");
    private final DBCollection containers = mongo.collection("containers");
    private final DBCollection programmeGroups = mongo.collection("programmeGroups");
    
    private final ContainerTranslator containerTranslator = new ContainerTranslator(new SubstitutionTableNumberCodec());
    private final ItemTranslator itemTranslator = new ItemTranslator(new SubstitutionTableNumberCodec());
    
    @BeforeClass
    public static void setUp() {
        mongo = MongoTestHelper.anEmptyTestDatabase();
    }
    
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
            
            Brand retrievedBrand = retrieveBrand(brand);
            
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
        
        Series retrievedSeries = retrieveSeries(series);
        assertEquals(item.getCanonicalUri(), Iterables.getOnlyElement(retrievedSeries.getChildRefs()).getUri());
        
        assertNull(containers.findOne(series.getCanonicalUri()));
        
        Brand retrievedBrand = retrieveBrand(brand);
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
        
        Series retrievedSeries = retrieveSeries(series);
        assertEquals(item.getCanonicalUri(), Iterables.getOnlyElement(retrievedSeries.getChildRefs()).getUri());
        
        assertNotNull(containers.findOne(series.getCanonicalUri()));
        
    }
    
    
    @Test
    public void testWritingContainer() {
        
        Brand container = new Brand("containerUri", "containerCurie", Publisher.BBC);
        
        contentWriter.createOrUpdate(container);
        
        assertNotNull(containers.findOne(container.getCanonicalUri()));
    }
    
    @Test
    public void testWritingContainerAndRemovingRelatedLinks() {
        Brand container = new Brand("containerUri", "containerCurie", Publisher.BBC);
        container.setRelatedLinks(ImmutableSet.of(RelatedLink.unknownTypeLink("http://example.org").build()));
        contentWriter.createOrUpdate(container);
        
        assertTrue(containers.findOne(container.getCanonicalUri()).containsField(DescribedTranslator.LINKS_KEY));
        
        container.setRelatedLinks(ImmutableSet.<RelatedLink>of());
        contentWriter.createOrUpdate(container);
        
        assertFalse(containers.findOne(container.getCanonicalUri()).containsField(DescribedTranslator.LINKS_KEY));
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
    
    @Test
    public void testConvertingTopLevelSeriesToBrandedSeries() {

        Series series = new Series("seriesUri","seriesCurie", Publisher.BBC);
        series.setThisOrChildLastUpdated(new DateTime(DateTimeZones.UTC));
        
        contentWriter.createOrUpdate(series);

        Series retrievedTopLevelSeries = retrieveTopLevelSeries(series);
        Series retrievedSeries = retrieveSeries(series);
        
        assertNotNull(retrievedTopLevelSeries);
        assertNotNull(retrievedSeries);
        
        Brand brand = new Brand("brandUri", "brandCurie", Publisher.BBC);
        
        series.setParent(brand);
        
        contentWriter.createOrUpdate(brand);
        contentWriter.createOrUpdate(series);
        
        Brand retrievedBrand = retrieveBrand(brand);

        assertNull("top-level series not null", containers.findOne(series.getCanonicalUri()));
        assertNotNull(programmeGroups.findOne(series.getCanonicalUri()));
        assertNotNull(retrievedBrand);
        assertTrue(retrievedBrand.getSeriesRefs().size() == 1);
    }
    
    @Test
    public void testThisOrChildLastUpdatedFieldIsKeptInSync() {
        
        DateTime brandLastUpdated = new DateTime(100, DateTimeZones.UTC);
        DateTime seriesLastUpdated = new DateTime(200, DateTimeZones.UTC);
        DateTime episodeLastUpdated = new DateTime(300, DateTimeZones.UTC);

        Brand brand = new Brand("brandUri", "brandUri", Publisher.BBC);
        brand.setLastUpdated(brandLastUpdated);
        
        contentWriter.createOrUpdate(brand);
        
        assertThat(retrieveBrand(brand).getThisOrChildLastUpdated(), is(equalTo(brandLastUpdated)));
        
        Series series = new Series("seriesUri","seriesCurie", Publisher.BBC);
        series.setLastUpdated(seriesLastUpdated );
        series.setParent(brand);
        
        contentWriter.createOrUpdate(series);
        
        assertThat(retrieveSeries(series).getThisOrChildLastUpdated(), is(equalTo(seriesLastUpdated)));
        assertThat(retrieveBrand(brand).getThisOrChildLastUpdated(), is(equalTo(seriesLastUpdated)));
        
        Episode episode = new Episode("itemUri", "itemCurie", Publisher.BBC);
        episode.setLastUpdated(episodeLastUpdated);
        episode.setSeries(series);
        episode.setContainer(brand);
        
        contentWriter.createOrUpdate(episode);
        
        assertThat(retrieveEpisode(episode).getThisOrChildLastUpdated(), is(equalTo(episodeLastUpdated)));
        assertThat(retrieveSeries(series).getThisOrChildLastUpdated(), is(equalTo(episodeLastUpdated)));
        assertThat(retrieveBrand(brand).getThisOrChildLastUpdated(), is(equalTo(episodeLastUpdated)));
        
        Episode episode2 = new Episode("itemUri2", "itemCurie2", Publisher.BBC);
        DateTime episodeLastUpdated2 = new DateTime(250, DateTimeZones.UTC);
        episode2.setLastUpdated(episodeLastUpdated2);
        episode2.setSeries(series);
        episode2.setContainer(brand);
        
        contentWriter.createOrUpdate(episode2);
        
        assertThat(retrieveEpisode(episode).getThisOrChildLastUpdated(), is(equalTo(episodeLastUpdated)));
        assertThat(retrieveEpisode(episode2).getThisOrChildLastUpdated(), is(equalTo(episodeLastUpdated2)));
        assertThat(retrieveSeries(series).getThisOrChildLastUpdated(), is(equalTo(episodeLastUpdated)));
        assertThat(retrieveBrand(brand).getThisOrChildLastUpdated(), is(equalTo(episodeLastUpdated)));
        
    }
    
    @Test
    public void testNumericIdsAreWrittenIntoParentAndChildRefs() {
        
        Brand brand = new Brand("brandUri", "brandUri", Publisher.BBC);
        brand.setId(1L);
        
        contentWriter.createOrUpdate(brand);
        
        Series series = new Series("seriesUri","seriesCurie", Publisher.BBC);
        series.setId(2L);
        series.withSeriesNumber(2);
        series.setParent(brand);
        
        contentWriter.createOrUpdate(series);
        
        Episode episode = new Episode("itemUri", "itemCurie", Publisher.BBC);
        episode.setId(3L);
        episode.setSeries(series);
        episode.setContainer(brand);

        contentWriter.createOrUpdate(episode);
        
        Brand retrievedBrand = retrieveBrand(brand);

        SeriesRef seriesRef = Iterables.getOnlyElement(retrievedBrand.getSeriesRefs());
        assertThat(seriesRef.getId(), is(2L));
        assertThat(seriesRef.getSeriesNumber(), is(2));
        
        ChildRef episodeRef = Iterables.getOnlyElement(retrievedBrand.getChildRefs());
        assertThat(episodeRef.getId(), is(3L));
        
        Series retrievedSeries = retrieveSeries(series);
        
        ChildRef seriesEpisodeRef = Iterables.getOnlyElement(retrievedSeries.getChildRefs());
        assertThat(seriesEpisodeRef.getId(), is(3L));
        
        ParentRef seriesBrandRef = retrievedSeries.getParent();
        assertThat(seriesBrandRef.getId(), is(1L));
        
        Episode retrievedEpisode = retrieveEpisode(episode);
        assertThat(retrievedEpisode.getContainer().getId(), is(1L));
        assertThat(retrievedEpisode.getSeriesRef().getId(), is(2L));
        
    }

    @Test
    public void testUpdateBrandRefForEpisodes(){
        Brand brand = new Brand("brandUri", "brandUri", Publisher.BBC);

        Series series = new Series("seriesUri","seriesCurie", Publisher.BBC);
        series.setParent(brand);

        Episode episode = new Episode("itemUri", "itemCurie", Publisher.BBC);
        episode.setContainer(brand);
        episode.setSeries(series);

        contentWriter.createOrUpdate(brand);
        contentWriter.createOrUpdate(series);
        contentWriter.createOrUpdate(episode);

        Episode retrievedEpisode = retrieveEpisode(episode);

        assertEquals(retrievedEpisode.getContainer().getUri(), "brandUri");

        Brand newBrand = new Brand("newBrandUri", "newBrandUri", Publisher.BBC);

        Series newSeries = new Series("seriesUri","seriesCurie", Publisher.BBC);
        newSeries.setParent(newBrand);

        Episode newEpisode = new Episode("itemUri", "itemCurie", Publisher.BBC);
        newEpisode.setContainer(newBrand);
        newEpisode.setSeries(newSeries);

        contentWriter.createOrUpdate(newBrand);
        contentWriter.createOrUpdate(newSeries);
        contentWriter.createOrUpdate(newEpisode);

        Episode retrievedNewEpisode = retrieveEpisode(newEpisode);

        assertEquals(retrievedNewEpisode.getContainer().getUri(), "newBrandUri");
    }

    @Test
    public void testWritingAndReadingCustomFields() {
        Brand brand = new Brand("brandUri", "brandUri", Publisher.BBC);
        brand.addCustomField("customField", "1");
        brand.addCustomField("customField2", "2");

        contentWriter.createOrUpdate(brand);

        Brand retrievedBrand = retrieveBrand(brand);

        assertEquals(brand.getCustomFields(), retrievedBrand.getCustomFields());
    }

    @Test
    public void testWritingAndReadingRatings() {
        Film film = new Film("filmUri", "filmUri", Publisher.IMDB);
        Rating ratingWithVotes = new Rating("10STAR", 8.7F, Publisher.IMDB, 1234L);
        film.setRatings(ImmutableSet.of(ratingWithVotes));

        contentWriter.createOrUpdate(film);

        Film retrievedFilm = retrieveFilm(film);

        Set<Rating> filmRatings = film.getRatings();
        Set<Rating> retrievedFilmRatings = retrievedFilm.getRatings();
        assertEquals(filmRatings, retrievedFilmRatings);
    }

    @Test
    public void testWritingAndReadingLocationProviders() {
        Film film = new Film("filmUri", "filmUri", Publisher.JUSTWATCH);
        Location location = new Location();
        location.setAvailable(true);
        location.setUri("locationUri");
        Provider provider = new Provider("Provider Name", "provider-icon.co.uk");
        location.setProvider(provider);

        Encoding encoding = new Encoding();
        encoding.setContainsAdvertising(false);
        encoding.addAvailableAt(location);

        Version version = new Version();
        version.setManifestedAs(Sets.newHashSet(encoding));

        film.addVersion(version);

        contentWriter.createOrUpdate(film);

        Film retrievedFilm = retrieveFilm(film);
        Set<Provider> retrievedProviders = new HashSet<>();
        for (Version v : retrievedFilm.getVersions()) {
            for (Encoding e : v.getManifestedAs()) {
                for(Location l : e.getAvailableAt()) {
                    retrievedProviders.add(location.getProvider());
                }
            }
        }
        assertEquals(1, retrievedProviders.size());
        assertEquals(provider, retrievedProviders.iterator().next());
    }

    public Brand retrieveBrand(Brand brand) {
        return (Brand) containerTranslator.fromDB(containers.findOne(brand.getCanonicalUri()));
    }

    public Series retrieveSeries(Series series) {
        return (Series) containerTranslator.fromDB(programmeGroups.findOne(series.getCanonicalUri()));
    }

    public Series retrieveTopLevelSeries(Series series) {
        return (Series) containerTranslator.fromDB(containers.findOne(series.getCanonicalUri()));
    }
    
    private Episode retrieveEpisode(Episode episode) {
        return (Episode) itemTranslator.fromDB(children.findOne(episode.getCanonicalUri()));
    }

    private Film retrieveFilm (Film film) {
        return (Film) itemTranslator.fromDB(topLevelItems.findOne(film.getCanonicalUri()));
    }
}
