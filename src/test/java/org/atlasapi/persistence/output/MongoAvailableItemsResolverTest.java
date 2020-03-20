package org.atlasapi.persistence.output;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metabroadcast.applications.client.model.internal.Application;
import com.metabroadcast.applications.client.model.internal.ApplicationConfiguration;
import com.metabroadcast.common.persistence.MongoTestHelper;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.persistence.mongo.DatabasedMongoClient;
import com.metabroadcast.common.time.DateTimeZones;
import com.metabroadcast.common.time.TimeMachine;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.ReadPreference;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.ChildRef;
import org.atlasapi.media.entity.Encoding;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Location;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.Policy;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Version;
import org.atlasapi.persistence.audit.NoLoggingPersistenceAuditLog;
import org.atlasapi.persistence.audit.PerHourAndDayMongoPersistenceAuditLog;
import org.atlasapi.persistence.audit.PersistenceAuditLog;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.mongo.MongoContentWriter;
import org.atlasapi.persistence.lookup.entry.EquivRefs;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.mongo.MongoLookupEntryStore;
import org.atlasapi.persistence.player.PlayerResolver;
import org.atlasapi.persistence.service.ServiceResolver;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Set;

import static org.atlasapi.persistence.lookup.entry.EquivRefs.Direction.OUTGOING;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class MongoAvailableItemsResolverTest {
    
    private static final ServiceResolver serviceResolver = mock(ServiceResolver.class);
    private static final PlayerResolver playerResolver = mock(PlayerResolver.class);

    private final MongoClient mongoClient = MongoTestHelper.anEmptyMongo();
    private final DatabasedMongo db = new DatabasedMongo(mongoClient, "testing");
    private final DatabasedMongoClient mongoDatabase = new DatabasedMongoClient(mongoClient, "testing");
    private final TimeMachine clock = new TimeMachine();
    private final MongoLookupEntryStore lookupStore
        = new MongoLookupEntryStore(
                mongoDatabase.collection("lookup", DBObject.class),
                mongoDatabase,
                new NoLoggingPersistenceAuditLog(),
            ReadPreference.primary()
    );
    private final MongoAvailableItemsResolver resolver
        = new MongoAvailableItemsResolver(db, lookupStore, clock);
    private final PersistenceAuditLog persistenceAuditLog = new PerHourAndDayMongoPersistenceAuditLog(db);
    private final ContentWriter writer = new MongoContentWriter(db, lookupStore, persistenceAuditLog,
            playerResolver, serviceResolver, clock);
    
    private final Brand primary = new Brand("primary", "primary", Publisher.BBC);
    private final Brand equivalent = new Brand("equivalent", "equivalent", Publisher.PA);
    
    private final Episode p1 = episode("p1", primary, location("p1l1", dateTime(0), dateTime(10)));
    private final Episode p2 = episode("p2", primary);
    
    private final Episode e1 = episode("e1", equivalent, location("e1l1", dateTime(0), dateTime(10)));
    private final Episode e2 = episode("e2", equivalent, location("e2l2", null, null)); // Availability start/end of null implies always available

    private Application application = mock(Application.class);
    private ApplicationConfiguration configWithoutPA;
    private ApplicationConfiguration configWithPA;
    
    @Before
    public void setUp() {
        writer.createOrUpdate(primary);
        writer.createOrUpdate(equivalent);
        writer.createOrUpdate(p1);
        writer.createOrUpdate(p2);
        writer.createOrUpdate(e1);
        writer.createOrUpdate(e2);
        
        writeEquivalences(p1, e1);
        writeEquivalences(p2, e2);
        
        primary.setEquivalentTo(ImmutableSet.of(LookupRef.from(equivalent)));
        p2.setEquivalentTo(ImmutableSet.of(LookupRef.from(p1)));

        configWithoutPA = ApplicationConfiguration.builder()
                .withPrecedence(ImmutableList.of())
                .withEnabledWriteSources(ImmutableList.of())
                .build();

        configWithPA = ApplicationConfiguration.builder()
                .withPrecedence(ImmutableList.of(Publisher.PA))
                .withEnabledWriteSources(ImmutableList.of())
                .build();
    }
    
    @Test
    public void testResolvesChildrenOfEquivalentBrandsAsTheItemsOfThePrimaryBrand() {
        clock.jumpTo(dateTime(5));

        when(application.getConfiguration()).thenReturn(configWithoutPA);

        Set<ChildRef> childRefs = ImmutableSet.copyOf(resolver.availableItemsFor(primary, application));
        ChildRef childRef = Iterables.getOnlyElement(childRefs);
        assertEquals(p1.getCanonicalUri(), childRef.getUri());

        when(application.getConfiguration()).thenReturn(configWithPA);

        childRefs = ImmutableSet.copyOf(resolver.availableItemsFor(primary, application));
        assertThat(childRefs.size(), is(2));
        assertThat(Iterables.transform(childRefs, ChildRef.TO_URI), hasItems(p1.getCanonicalUri(), p2.getCanonicalUri()));
        
    }

    private void writeEquivalences(Episode a, Episode b) {
        LookupEntry aEntry = LookupEntry.lookupEntryFrom(a)
            .copyWithDirectEquivalents(EquivRefs.of(LookupRef.from(b), OUTGOING))
            .copyWithEquivalents(ImmutableSet.of(LookupRef.from(b)));
        LookupEntry bEntry = LookupEntry.lookupEntryFrom(b)
            .copyWithDirectEquivalents(EquivRefs.of(LookupRef.from(a), OUTGOING))
            .copyWithEquivalents(ImmutableSet.of(LookupRef.from(a)));
        
        lookupStore.store(aEntry);
        lookupStore.store(bEntry);
        
    }

    private Episode episode(String uri, Brand container, Location... locations) {
        Episode episode = new Episode();
        episode.setCanonicalUri(uri);
        episode.setContainer(container);
        episode.setPublisher(container.getPublisher());
        
        Version version = new Version();
        Encoding encoding = new Encoding();
        encoding.setAvailableAt(ImmutableSet.copyOf(locations));
        version.setManifestedAs(ImmutableSet.of(encoding));
        episode.setVersions(ImmutableSet.of(version));
        return episode;
    }

    private Location location(String uri, DateTime start, DateTime end) {
        Location location = new Location();
        location.setUri(uri);
        Policy policy = new Policy();
        policy.setAvailabilityStart(start);
        policy.setAvailabilityEnd(end);
        location.setPolicy(policy);
        return location;
    }

    private DateTime dateTime(int millis) {
        return new DateTime(millis,DateTimeZones.UTC);
    }
    
}
