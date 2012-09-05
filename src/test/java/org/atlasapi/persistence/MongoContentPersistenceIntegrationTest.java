package org.atlasapi.persistence;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.KnownTypeContentResolver;
import org.atlasapi.persistence.logging.MongoLoggingAdapter;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.persistence.MongoTestHelper;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.mongodb.Mongo;

public class MongoContentPersistenceIntegrationTest {

        private final Mongo mongo = MongoTestHelper.anEmptyMongo();
        private final DatabasedMongo db = new DatabasedMongo(mongo, "atlas");
        private final MongoLoggingAdapter adapterLog = new MongoLoggingAdapter(db);
    
    @Test
    public void test() {
        
        MongoContentPersistenceModule module = new MongoContentPersistenceModule(mongo, db, adapterLog);
        
        ContentWriter contentWriter = module.contentWriter();
        ContentResolver contentResolver = module.contentResolver();
        LookupEntryStore lookupStore = module.lookupStore();
        KnownTypeContentResolver knownTypeContentResolver = module.knownTypeContentResolver();
        
        String uri = "itemUri";
        Item item = new Item(uri, "itemCurie", Publisher.BBC);
        item.setTitle("I am a title");
        
        contentWriter.createOrUpdate(item);
        
        Iterable<LookupEntry> entries = lookupStore.entriesForCanonicalUris(ImmutableSet.of(item.getCanonicalUri()));
        LookupEntry entry = Iterables.getOnlyElement(entries);
        Long id = entry.id();
        assertThat(id, is(nullValue()));

        Maybe<Identified> maybeContent = knownTypeContentResolver.findByLookupRefs(ImmutableSet.of(entry.lookupRef())).get(uri);
        assertThat((Item) maybeContent.requireValue(), is(equalTo(item)));
        assertThat(((Item) maybeContent.requireValue()).getId(), is(equalTo(id)));
        assertThat(((Item) maybeContent.requireValue()).getTitle(), is(equalTo(item.getTitle())));

        Maybe<Identified> moreContent = contentResolver.findByCanonicalUris(ImmutableSet.of(uri)).get(uri);
        assertThat((Item) moreContent.requireValue(), is(equalTo(item)));
        assertThat(((Item) moreContent.requireValue()).getId(), is(equalTo(id)));
        assertThat(((Item) moreContent.requireValue()).getTitle(), is(equalTo(item.getTitle())));
        
        String newTitle = "Changed title";
        item.setTitle(newTitle);
        item.setId(1234L);
        
        contentWriter.createOrUpdate(item);
        
        moreContent = contentResolver.findByCanonicalUris(ImmutableSet.of(uri)).get(uri);
        assertThat((Item) moreContent.requireValue(), is(equalTo(item)));
        //ID setting is currently disabled, so this check is not valid
        //assertThat(((Item) moreContent.requireValue()).getStringId(), is(equalTo(id)));
        assertThat(((Item) moreContent.requireValue()).getTitle(), is(equalTo(newTitle)));
        
    }

}
