package org.atlasapi.application.persistence;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.util.List;
import org.atlasapi.application.Application;
import org.atlasapi.application.ApplicationCredentials;
import org.atlasapi.application.ApplicationSources;
import org.atlasapi.application.SourceReadEntry;
import org.atlasapi.media.common.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.application.ApplicationStore;
import org.atlasapi.persistence.application.MongoApplicationStore;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.metabroadcast.common.ids.IdGenerator;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.persistence.MongoTestHelper;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.time.DateTimeZones;

public class MongoApplicationStoreTest {
    private final NumberToShortStringCodec idCodec = SubstitutionTableNumberCodec.lowerCaseOnly();
    private DatabasedMongo adminMongo;
    private ApplicationStore store;
    
    @Before
    public void setup() {

        IdGenerator idGenerator = mock(IdGenerator.class);
        when(idGenerator.generateRaw()).thenReturn(5004L);
        adminMongo = MongoTestHelper.anEmptyTestDatabase();
        store = new MongoApplicationStore(idGenerator, idCodec, adminMongo);
    }
    
    @Test
    public void testEncodesAndDecodesApplication() {
        
        final Id applicationId = Id.valueOf(5000);
        final String slug = "app-5000";
        final String title = "test application";
        final String apiKey = "abc123";
        final DateTime created = new DateTime(DateTimeZones.UTC)
               .withDate(2013, 9, 13)
               .withTime(15, 13, 0, 0);
        final SourceReadEntry testEntry1 = new SourceReadEntry(Publisher.BBC, Publisher.BBC.getDefaultSourceStatus());
        final SourceReadEntry testEntry2 = new SourceReadEntry(Publisher.NETFLIX, Publisher.NETFLIX.getDefaultSourceStatus());
        List<SourceReadEntry> reads = ImmutableList.of(testEntry1, testEntry2);
        List<Publisher> writes = ImmutableList.of(Publisher.KANDL_TOPICS, Publisher.DBPEDIA);
        ApplicationSources sources = ApplicationSources.builder()
                .withPrecedence(true)
                .withReads(reads)
                .withWrites(writes)
                .build();
        ApplicationCredentials credentials = ApplicationCredentials.builder()
                .withApiKey(apiKey).build();
        
        Application application = Application.builder()
                .withId(applicationId)
                .withSlug(slug)
                .withTitle(title)
                .withCreated(created)
                .withCredentials(credentials)
                .withSources(sources)
                .build();
        
        store.updateApplication(application);
        Optional<Application> retrieved = store.applicationFor(applicationId);
        if (!retrieved.isPresent()) {
            fail("No application");
            return;
        }
        // top level
        assertEquals(applicationId, retrieved.get().getId());
        assertEquals(slug, retrieved.get().getSlug());
        assertEquals(title, retrieved.get().getTitle());
        assertEquals(created, retrieved.get().getCreated());
        // Credentials
        assertEquals(apiKey, retrieved.get().getCredentials().getApiKey());
        // Sources - Reads
        assertTrue(retrieved.get().getSources().isPrecedenceEnabled());
        assertEquals(2, retrieved.get().getSources().getReads().size());
        SourceReadEntry retrievedEntry1 = retrieved.get().getSources().getReads().get(0);
        assertEquals(testEntry1.getPublisher(), retrievedEntry1.getPublisher());
        assertEquals(testEntry1.getSourceStatus(), retrievedEntry1.getSourceStatus());
        SourceReadEntry retrievedEntry2 = retrieved.get().getSources().getReads().get(1);
        assertEquals(testEntry2.getPublisher(), retrievedEntry2.getPublisher());
        assertEquals(testEntry2.getSourceStatus(), retrievedEntry2.getSourceStatus());
        // Sources - Writes
        assertTrue(retrieved.get().getSources().getWrites().contains(Publisher.KANDL_TOPICS));
        assertTrue(retrieved.get().getSources().getWrites().contains(Publisher.DBPEDIA));
        assertFalse(retrieved.get().getSources().getWrites().contains(Publisher.BBC));
    }
    
    @Test
    public void testAddIdAndApiKey() {
        final String title = "test application for api key";
        final DateTime created = new DateTime(DateTimeZones.UTC)
               .withDate(2013, 9, 13)
               .withTime(15, 13, 0, 0);
        final SourceReadEntry testEntry1 = new SourceReadEntry(Publisher.BBC, Publisher.BBC.getDefaultSourceStatus());
        final SourceReadEntry testEntry2 = new SourceReadEntry(Publisher.NETFLIX, Publisher.NETFLIX.getDefaultSourceStatus());
        List<SourceReadEntry> reads = ImmutableList.of(testEntry1, testEntry2);
        List<Publisher> writes = ImmutableList.of(Publisher.KANDL_TOPICS, Publisher.DBPEDIA);
        ApplicationSources sources = ApplicationSources.builder()
                .withPrecedence(true)
                .withReads(reads)
                .withWrites(writes)
                .build();
        ApplicationCredentials credentials = ApplicationCredentials.builder()
                .withApiKey("").build();
        
        Application application = Application.builder()
                .withTitle(title)
                .withCreated(created)
                .withCredentials(credentials)
                .withSources(sources)
                .build();
        store.createApplication(application);
        //retrieve
        Optional<Application> retrieved = store.applicationFor(Id.valueOf(5004));
        if (!retrieved.isPresent()) {
            fail("No application");
            return;
        }
        assertEquals(Id.valueOf(5004), retrieved.get().getId());
        assertEquals(title, retrieved.get().getTitle());
        assertFalse(retrieved.get().getCredentials().getApiKey().isEmpty());
    }
    
    @After
    public void tearDown() {
        MongoTestHelper.clearDB();
    }
}
