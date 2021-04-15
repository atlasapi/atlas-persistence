package org.atlasapi.persistence.content.mongo;

import java.util.List;

import org.atlasapi.content.criteria.ContentQuery;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Series;
import org.atlasapi.persistence.audit.PerHourAndDayMongoPersistenceAuditLog;
import org.atlasapi.persistence.audit.PersistenceAuditLog;
import org.atlasapi.persistence.lookup.NewLookupWriter;
import org.atlasapi.persistence.lookup.mongo.MongoLookupEntryStore;
import org.atlasapi.persistence.player.PlayerResolver;
import org.atlasapi.persistence.service.ServiceResolver;

import com.metabroadcast.applications.client.model.internal.Application;
import com.metabroadcast.common.persistence.MongoTestHelper;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.persistence.mongo.DatabasedMongoClient;
import com.metabroadcast.common.query.Selection;
import com.metabroadcast.common.time.SystemClock;

import com.google.common.collect.ImmutableList;
import com.mongodb.MongoClient;
import com.mongodb.ReadPreference;
import junit.framework.TestCase;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.Mockito.mock;

public class MongoContentResolverTest extends TestCase {
    private static final Item noSeriesItem = new Item("noSeriesItem","noSeriesItem", Publisher.TESTING_MBST);
    private static final Episode s1e1 = new Episode("s1e1", "s1e1", Publisher.TESTING_MBST);
    private static final Episode s1e2 = new Episode("s1e2", "s1e2", Publisher.TESTING_MBST);
    private static final Episode s2e1 = new Episode("s2e1", "s2e1", Publisher.TESTING_MBST);
    private static final Episode noSeriesEpisode = new Episode("noSeriesEpisode", "noSeriesEpisode", Publisher.TESTING_MBST);
    private static final Episode noContainerEpisode = new Episode("noContainerEpisode", "noContainerEpisode", Publisher.TESTING_MBST);
    private static final Series s1 = new Series("s1", "s1", Publisher.TESTING_MBST);
    private static final Series s2 = new Series("s2", "s2", Publisher.TESTING_MBST);
    private static final Series topLevelSeries = new Series("noContainerSeries", "noContainerSeries", Publisher.TESTING_MBST);
    private static final Episode topLevelSeriesEpisode = new Episode("noContainerSeriesEpisode", "noContainerSeriesEpisode", Publisher.TESTING_MBST);
    private static final Brand brand= new Brand("brand", "brand", Publisher.TESTING_MBST);

    private static final MongoClient mongoClient = MongoTestHelper.anEmptyMongo();
    private static final DatabasedMongo mongo = new DatabasedMongo(mongoClient, "testing");
    private static final DatabasedMongoClient mongoDatabase = new DatabasedMongoClient(mongoClient, "testing");
    private static final PersistenceAuditLog persistenceAuditLog = new PerHourAndDayMongoPersistenceAuditLog(mongo);

    private static final ServiceResolver serviceResolver = mock(ServiceResolver.class);
    private static final PlayerResolver playerResolver = mock(PlayerResolver.class);
    private static final ContentQuery matchEverythingContentQuery = new ContentQuery(ImmutableList.of(), Selection.ALL, mock(
            Application.class));

    private final MongoContentResolver resolver =  new MongoContentResolver(mongo, new MongoLookupEntryStore(
            mongoDatabase,
            "lookup",
            persistenceAuditLog,
            ReadPreference.primary()));

    @Before
    public void writeTestContents() {



    }

    //Apologies for the massive test we are in a hurry here!
    @Test
    public void testFindChildrenForParent() {
        NewLookupWriter lookupStore = new NewLookupWriter() {
            @Override
            public void ensureLookup(Content described) {
            }
        };
        MongoContentWriter writer = new MongoContentWriter(
                mongo,
                lookupStore,
                persistenceAuditLog,
                playerResolver,
                serviceResolver,
                new SystemClock());

        s1e1.setSeries(s1);
        s1e1.setContainer(brand);
        s1e2.setSeries(s1);
        s1e2.setContainer(brand);
        s2e1.setSeries(s2);
        s2e1.setContainer(brand);
        noSeriesItem.setContainer(brand);
        noSeriesEpisode.setContainer(brand);
        noContainerEpisode.setSeries(s1);
        s1.setParent(brand);
        s2.setParent(brand);
        topLevelSeriesEpisode.setSeries(topLevelSeries);
        topLevelSeriesEpisode.setContainer(topLevelSeries);
        s1.withSeriesNumber(1);
        s2.withSeriesNumber(2);
        s1e1.setEpisodeNumber(1);
        s1e2.setEpisodeNumber(2);
        s2e1.setEpisodeNumber(1);
        noSeriesEpisode.setEpisodeNumber(1);
        noContainerEpisode.setEpisodeNumber(1);

        writer.createOrUpdate(brand);
        writer.createOrUpdate(s1);
        writer.createOrUpdate(s2);
        writer.createOrUpdate(noSeriesItem);
        writer.createOrUpdate(s1e1);
        writer.createOrUpdate(s1e2);
        writer.createOrUpdate(s2e1);
        writer.createOrUpdate(noSeriesEpisode);
        writer.createOrUpdate(noContainerEpisode);
        writer.createOrUpdate(topLevelSeries);
        writer.createOrUpdate(topLevelSeriesEpisode);

        List<Identified> childrenForBrand = resolver.findChildrenForParent(brand).getAllResolvedResults();
        List<Identified> expectedChildrenForBrand = ImmutableList.of(noSeriesEpisode, noSeriesItem, s1e1, s1e2, s2e1);

        assertTrue("childrenFors1 does not contain the correct items.",
                expectedChildrenForBrand.size() == childrenForBrand.size()
                   && expectedChildrenForBrand.containsAll(childrenForBrand)
                   && childrenForBrand.containsAll(expectedChildrenForBrand));

        List<Identified> childrenForS1 = resolver.findChildrenForParent(s1).getAllResolvedResults();
        List<Identified> expectedChildrenForS1 = ImmutableList.of(s1e1, s1e2, noContainerEpisode);

        assertTrue("childrenForS2 does not contain the correct items.",
                expectedChildrenForS1.size() == childrenForS1.size()
                && expectedChildrenForS1.containsAll(childrenForS1)
                && childrenForS1.containsAll(expectedChildrenForS1));

        List<Identified> childrenForS2 = resolver.findChildrenForParent(s2).getAllResolvedResults();
        List<Identified> expectedChildrenForS2 = ImmutableList.of(s2e1);

        assertTrue("childrenForS2 does not contain the correct items.",
                expectedChildrenForS2.size() == childrenForS2.size()
                && expectedChildrenForS2.containsAll(childrenForS2)
                && childrenForS2.containsAll(expectedChildrenForS2));

        List<Identified> seriesForBrand = resolver.findSeriesForBrand(brand).getAllResolvedResults();
        List<Identified> expectedseriesForBrand = ImmutableList.of(s1, s2);

        assertTrue("seriesForBrand does not contain the correct items.",
                seriesForBrand.size() == expectedseriesForBrand.size()
                && seriesForBrand.containsAll(expectedseriesForBrand)
                && expectedseriesForBrand.containsAll(seriesForBrand));

        //TODO: this only tests the case where the top level series is referenced as a series.
        //if the top level series is referenced as a container it is unclear how the method should
        //be dealing with it.
        List<Identified> childrenForTopLevelSeries = resolver.findChildrenForParent(topLevelSeries).getAllResolvedResults();
        List<Identified> expectedChildrenForTopLevelSeries = ImmutableList.of(topLevelSeriesEpisode);

        assertTrue("childrenForTopLevelSeries does not contain the correct items.",
                childrenForTopLevelSeries.size() == expectedChildrenForTopLevelSeries.size()
                && childrenForTopLevelSeries.containsAll(expectedChildrenForTopLevelSeries)
                && expectedChildrenForTopLevelSeries.containsAll(childrenForTopLevelSeries));


    }
}