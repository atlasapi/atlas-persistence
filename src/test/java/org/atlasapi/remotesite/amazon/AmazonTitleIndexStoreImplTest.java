package org.atlasapi.remotesite.amazon;

import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.persistence.MongoTestHelper;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import org.atlasapi.remotesite.amazon.indexer.AmazonTitleIndexEntry;
import org.atlasapi.remotesite.amazon.indexer.AmazonTitleIndexStoreImpl;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class AmazonTitleIndexStoreImplTest {

    private AmazonTitleIndexStoreImpl amazonTitleIndexStoreImpl;

    @Before
    public void setUp() {
        DatabasedMongo mongo = MongoTestHelper.anEmptyTestDatabase();
        amazonTitleIndexStoreImpl = new AmazonTitleIndexStoreImpl(mongo);
    }

    @Test
    public void testRetrievingTitleIndexEntryIsEqual() {
        AmazonTitleIndexEntry entry = new AmazonTitleIndexEntry("title", ImmutableSet.of("uri1", "uri2"));
        amazonTitleIndexStoreImpl.createOrUpdateIndex(entry);
        AmazonTitleIndexEntry retrievedEntry = amazonTitleIndexStoreImpl.getIndexEntry(entry.getTitle());
        assertThat(retrievedEntry, is(entry));
    }

}