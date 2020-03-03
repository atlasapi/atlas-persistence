package org.atlasapi.persistence.lookup.entry;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentCategory;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

import static org.atlasapi.persistence.lookup.entry.EquivRefs.EquivDirection.BIDIRECTIONAL;
import static org.atlasapi.persistence.lookup.entry.EquivRefs.EquivDirection.INCOMING;
import static org.atlasapi.persistence.lookup.entry.EquivRefs.EquivDirection.OUTGOING;
import static org.junit.Assert.assertEquals;

public class LookupEntryTest {

    private LookupRef self;
    private LookupRef ref1;
    private LookupRef ref2;
    private LookupRef ref3;
    private LookupRef ref4;
    private LookupRef ref5;
    private LookupRef ref6;
    private LookupEntry lookupEntry;
    private EquivRefs directs;
    private EquivRefs explicits;
    private EquivRefs blacklisted;

    @Before
    public void setUp() {
        self = ref(0);
        ref1 = ref(1);
        ref2 = ref(2);
        ref3 = ref(3);
        ref4 = ref(4);
        ref5 = ref(5);
        ref6 = ref(6);

        directs = EquivRefs.of(
                ImmutableMap.of(
                        ref1, INCOMING,
                        ref2, OUTGOING,
                        ref3, BIDIRECTIONAL
                )
        );

        explicits = EquivRefs.of(
                ImmutableMap.of(
                        ref4, INCOMING,
                        ref5, OUTGOING,
                        ref6, BIDIRECTIONAL
                )
        );

        blacklisted = EquivRefs.of();

        lookupEntry = entry(self, directs, explicits, blacklisted);
    }

    private LookupRef ref(long id) {
        return new LookupRef("ref" + id, id, Publisher.METABROADCAST, ContentCategory.TOP_LEVEL_ITEM);
    }

    private LookupEntry entry(LookupRef self, EquivRefs directs, EquivRefs explicits, EquivRefs blacklisted) {
        return new LookupEntry(
                self.uri(),
                self.id(),
                self,
                ImmutableSet.of(),
                ImmutableSet.of(),
                directs,
                explicits,
                blacklisted,
                ImmutableSet.of(),
                DateTime.now(),
                DateTime.now(),
                DateTime.now(),
                true
        );
    }

    @Test
    public void testGetOutgoing() {
        assertEquals(ImmutableSet.of(ref2, ref3, ref5, ref6), lookupEntry.getOutgoing());
        blacklisted = EquivRefs.of(
                ImmutableMap.of(
                        ref2, OUTGOING,
                        ref5, BIDIRECTIONAL,
                        ref6, INCOMING
                )
        );
        LookupEntry newLookupEntry = entry(self, directs, explicits, blacklisted);
        assertEquals(ImmutableSet.of(ref3, ref6), newLookupEntry.getOutgoing());
    }

    @Test
    public void testGetIncoming() {
        assertEquals(ImmutableSet.of(ref1, ref3, ref4, ref6), lookupEntry.getIncoming());
        blacklisted = EquivRefs.of(
                ImmutableMap.of(
                        ref1, INCOMING,
                        ref4, BIDIRECTIONAL,
                        ref6, OUTGOING
                )
        );
        LookupEntry newLookupEntry = entry(self, directs, explicits, blacklisted);
        assertEquals(ImmutableSet.of(ref3, ref6), newLookupEntry.getIncoming());
    }

    @Test
    public void testGetNeighbours() {
        assertEquals(ImmutableSet.of(ref1, ref2, ref3, ref4, ref5, ref6), lookupEntry.getNeighbours());
        blacklisted = EquivRefs.of(
                ImmutableMap.of(
                        ref2, OUTGOING,
                        ref5, BIDIRECTIONAL,
                        ref6, INCOMING
                )
        );
        LookupEntry newLookupEntry = entry(self, directs, explicits, blacklisted);
        assertEquals(ImmutableSet.of(ref1, ref3, ref4, ref6), newLookupEntry.getNeighbours());
    }
}