package org.atlasapi.persistence.lookup.mongo;

import com.google.common.collect.ImmutableSet;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentCategory;
import org.atlasapi.persistence.lookup.entry.EquivRefs;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.joda.time.DateTime;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class LookupEntryHasherTest {

    private final LookupEntryHasher hasher = new LookupEntryHasher(new LookupEntryTranslator());
    
    @Test
    public void testHashDoesntChangeWhenAuditTimestampsChange() {
        DateTime created = DateTime.now();
        DateTime updated = created.plusHours(1);
        LookupEntry lookupEntry = createLookupEntry(created, created);
        LookupEntry lookupEntryWithDifferentUpdated = createLookupEntry(updated, updated);
        
        assertEquals(hasher.writeHashFor(lookupEntry), hasher.writeHashFor(lookupEntryWithDifferentUpdated));
    }
    
    private LookupEntry createLookupEntry(DateTime created, DateTime updated) {
        String uri = "http://example.org";
        long id = 1;
        Publisher publisher = Publisher.METABROADCAST;

        return new LookupEntry(
                uri,
                id,
                new LookupRef(uri, id, publisher, ContentCategory.TOP_LEVEL_ITEM),
                ImmutableSet.of(),
                ImmutableSet.of(),
                EquivRefs.of(),
                EquivRefs.of(),
                EquivRefs.of(),
                ImmutableSet.of(),
                created,
                updated,
                updated,
                true
        );
                
                
                
    }
}
