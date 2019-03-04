package org.atlasapi.remotesite.amazon;

import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.persistence.MongoTestHelper;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import org.atlasapi.remotesite.amazon.indexer.AmazonTitleIndexEntry;
import org.atlasapi.remotesite.amazon.indexer.AmazonTitleIndexStore;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class AmazonTitleIndexStoreTest {

    private AmazonTitleIndexStore amazonTitleIndexStore;

    @Before
    public void setUp() {
        DatabasedMongo mongo = MongoTestHelper.anEmptyTestDatabase();
        amazonTitleIndexStore = new AmazonTitleIndexStore(mongo);
    }

    @Test
    public void testRetrievingTitleIndexEntryIsEqual() {
        AmazonTitleIndexEntry entry = new AmazonTitleIndexEntry("title", ImmutableSet.of("uri1", "uri2"));
        amazonTitleIndexStore.createOrUpdateIndex(entry);
        AmazonTitleIndexEntry retrievedEntry = amazonTitleIndexStore.getIndexEntry(entry.getTitle());
        assertThat(retrievedEntry, is(entry));
    }

}