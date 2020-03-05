package org.atlasapi.persistence.lookup.entry;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentCategory;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.atlasapi.persistence.lookup.entry.EquivRefs.EquivDirection;
import static org.atlasapi.persistence.lookup.entry.EquivRefs.EquivDirection.BIDIRECTIONAL;
import static org.atlasapi.persistence.lookup.entry.EquivRefs.EquivDirection.INCOMING;
import static org.atlasapi.persistence.lookup.entry.EquivRefs.EquivDirection.OUTGOING;
import static org.atlasapi.persistence.lookup.entry.EquivRefs.of;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class EquivRefsTest {

    private LookupRef ref1;
    private LookupRef ref2;
    private LookupRef ref3;
    private LookupRef ref4;
    private EquivRefs equivRefs;
    private Map<LookupRef, EquivDirection> equivRefsMap;

    @Before
    public void setUp() {
        ref1 = ref(1);
        ref2 = ref(2);
        ref3 = ref(3);
        ref4 = ref(4);
        equivRefsMap = ImmutableMap.of(
                ref1, INCOMING,
                ref2, OUTGOING,
                ref3, BIDIRECTIONAL
        );
        equivRefs = of(equivRefsMap);
    }

    private LookupRef ref(long id) {
        return new LookupRef("ref" + id, id, Publisher.METABROADCAST, ContentCategory.TOP_LEVEL_ITEM);
    }

    @Test
    public void testOfAndGetEquivRefsReturnExpectedMap() {
        assertEquals(equivRefsMap, equivRefs.getEquivRefs());
        equivRefs = of(ref1, OUTGOING);
        assertEquals(ImmutableMap.of(ref1, OUTGOING), equivRefs.getEquivRefs());
        equivRefs = of(ImmutableSet.of(ref1, ref2), INCOMING);
        assertEquals(ImmutableMap.of(ref1, INCOMING, ref2, INCOMING), equivRefs.getEquivRefs());
    }

    @Test
    public void getLookupRefs() {
        assertEquals(equivRefsMap.keySet(), equivRefs.getLookupRefs());
    }

    @Test
    public void contains() {
        assertTrue(equivRefs.contains(ref1));
        assertTrue(equivRefs.contains(ref2));
        assertTrue(equivRefs.contains(ref3));

        assertTrue(equivRefs.contains(ref1, INCOMING));
        assertTrue(equivRefs.contains(ref2, OUTGOING));
        assertTrue(equivRefs.contains(ref3, BIDIRECTIONAL));

        assertTrue(equivRefs.contains(ref3, INCOMING));
        assertTrue(equivRefs.contains(ref3, OUTGOING));

        assertFalse(equivRefs.contains(ref1, OUTGOING));
        assertFalse(equivRefs.contains(ref1, BIDIRECTIONAL));
    }

    @Test
    public void testGetOutgoing() {
        assertEquals(ImmutableSet.of(ref2, ref3), equivRefs.getOutgoing());
    }

    @Test
    public void testGetIncoming() {
        assertEquals(ImmutableSet.of(ref1, ref3), equivRefs.getIncoming());
    }

    @Test
    public void testCopyWithLink() {
        EquivRefs newEquivRefs = equivRefs.copyWithLink(ref1, INCOMING);
        assertEquals(equivRefs, newEquivRefs);
        newEquivRefs = equivRefs.copyWithLink(ref2, OUTGOING);
        assertEquals(equivRefs, newEquivRefs);
        newEquivRefs = equivRefs.copyWithLink(ref3, INCOMING);
        assertEquals(equivRefs, newEquivRefs);
        newEquivRefs = equivRefs.copyWithLink(ref3, BIDIRECTIONAL);
        assertEquals(equivRefs, newEquivRefs);
        newEquivRefs = equivRefs.copyWithLink(ref3, BIDIRECTIONAL);
        assertEquals(equivRefs, newEquivRefs);

        newEquivRefs = equivRefs.copyWithLink(ref1, OUTGOING);
        Map<LookupRef, EquivDirection> expectedEquivRefsMap = ImmutableMap.of(
            ref1, BIDIRECTIONAL,
            ref2, OUTGOING,
            ref3, BIDIRECTIONAL
        );
        assertEquals(EquivRefs.of(expectedEquivRefsMap), newEquivRefs);

        newEquivRefs = equivRefs.copyWithLink(ref2, INCOMING);
        expectedEquivRefsMap = ImmutableMap.of(
                ref1, INCOMING,
                ref2, BIDIRECTIONAL,
                ref3, BIDIRECTIONAL
        );
        assertEquals(EquivRefs.of(expectedEquivRefsMap), newEquivRefs);


        newEquivRefs = equivRefs.copyWithLink(ref4, INCOMING);
        expectedEquivRefsMap = ImmutableMap.<LookupRef, EquivDirection>builder()
                .putAll(equivRefsMap)
                .put(ref4, INCOMING)
                .build();
        assertEquals(EquivRefs.of(expectedEquivRefsMap), newEquivRefs);

        newEquivRefs = equivRefs.copyWithLink(ref4, OUTGOING);
        expectedEquivRefsMap = ImmutableMap.<LookupRef, EquivDirection>builder()
                .putAll(equivRefsMap)
                .put(ref4, OUTGOING)
                .build();
        assertEquals(EquivRefs.of(expectedEquivRefsMap), newEquivRefs);

        newEquivRefs = equivRefs.copyWithLink(ref4, BIDIRECTIONAL);
        expectedEquivRefsMap = ImmutableMap.<LookupRef, EquivDirection>builder()
                .putAll(equivRefsMap)
                .put(ref4, BIDIRECTIONAL)
                .build();
        assertEquals(EquivRefs.of(expectedEquivRefsMap), newEquivRefs);

    }

    @Test
    public void testCopyWithLinks() {
        EquivRefs newEquivRefs = equivRefs.copyWithLinks(
                ImmutableSet.of(ref1, ref2, ref3, ref4), INCOMING
        );
        Map<LookupRef, EquivDirection> expectedEquivRefsMap = ImmutableMap.of(
                ref1, INCOMING,
                ref2, BIDIRECTIONAL,
                ref3, BIDIRECTIONAL,
                ref4, INCOMING
        );

        assertEquals(EquivRefs.of(expectedEquivRefsMap), newEquivRefs);

        newEquivRefs = equivRefs.copyWithLinks(
                ImmutableMap.of(
                        ref1, INCOMING,
                        ref2, INCOMING,
                        ref4, OUTGOING
                )
        );

        expectedEquivRefsMap = ImmutableMap.of(
                ref1, INCOMING,
                ref2, BIDIRECTIONAL,
                ref3, BIDIRECTIONAL,
                ref4, OUTGOING
        );

        assertEquals(EquivRefs.of(expectedEquivRefsMap), newEquivRefs);
    }

    @Test
    public void testCopyWithoutLink() {
        EquivRefs newEquivRefs = equivRefs.copyWithoutLink(ref1, OUTGOING);
        assertEquals(equivRefs, newEquivRefs);
        newEquivRefs = equivRefs.copyWithoutLink(ref2, INCOMING);
        assertEquals(equivRefs, newEquivRefs);
        newEquivRefs = equivRefs.copyWithoutLink(ref4, INCOMING);
        assertEquals(equivRefs, newEquivRefs);
        newEquivRefs = equivRefs.copyWithoutLink(ref4, OUTGOING);
        assertEquals(equivRefs, newEquivRefs);
        newEquivRefs = equivRefs.copyWithoutLink(ref4, BIDIRECTIONAL);
        assertEquals(equivRefs, newEquivRefs);

        newEquivRefs = equivRefs.copyWithoutLink(ref1, INCOMING);
        Map<LookupRef, EquivDirection> expectedEquivRefsMap = ImmutableMap.of(
                ref2, OUTGOING,
                ref3, BIDIRECTIONAL
        );
        assertEquals(EquivRefs.of(expectedEquivRefsMap), newEquivRefs);
        newEquivRefs = equivRefs.copyWithoutLink(ref1, BIDIRECTIONAL);
        assertEquals(EquivRefs.of(expectedEquivRefsMap), newEquivRefs);

        newEquivRefs = equivRefs.copyWithoutLink(ref2, OUTGOING);
        expectedEquivRefsMap = ImmutableMap.of(
                ref1, INCOMING,
                ref3, BIDIRECTIONAL
        );
        assertEquals(EquivRefs.of(expectedEquivRefsMap), newEquivRefs);
        newEquivRefs = equivRefs.copyWithoutLink(ref2, BIDIRECTIONAL);
        assertEquals(EquivRefs.of(expectedEquivRefsMap), newEquivRefs);


        newEquivRefs = equivRefs.copyWithoutLink(ref3, INCOMING);
        expectedEquivRefsMap = ImmutableMap.of(
                ref1, INCOMING,
                ref2, OUTGOING,
                ref3, OUTGOING
        );
        assertEquals(EquivRefs.of(expectedEquivRefsMap), newEquivRefs);

        newEquivRefs = equivRefs.copyWithoutLink(ref3, OUTGOING);
        expectedEquivRefsMap = ImmutableMap.of(
                ref1, INCOMING,
                ref2, OUTGOING,
                ref3, INCOMING
        );
        assertEquals(EquivRefs.of(expectedEquivRefsMap), newEquivRefs);

        newEquivRefs = equivRefs.copyWithoutLink(ref3, BIDIRECTIONAL);
        expectedEquivRefsMap = ImmutableMap.of(
                ref1, INCOMING,
                ref2, OUTGOING
        );
        assertEquals(EquivRefs.of(expectedEquivRefsMap), newEquivRefs);

    }

    @Test
    public void testCopyWithoutLinks() {
        EquivRefs newEquivRefs = equivRefs.copyWithoutLinks(
                ImmutableSet.of(ref1, ref2, ref3, ref4), INCOMING
        );
        Map<LookupRef, EquivDirection> expectedEquivRefsMap = ImmutableMap.of(
                ref2, OUTGOING,
                ref3, OUTGOING
        );

        assertEquals(EquivRefs.of(expectedEquivRefsMap), newEquivRefs);

        newEquivRefs = equivRefs.copyWithoutLinks(
                ImmutableMap.of(
                        ref1, BIDIRECTIONAL,
                        ref2, OUTGOING,
                        ref4, INCOMING
                )
        );

        expectedEquivRefsMap = ImmutableMap.of(
                ref3, BIDIRECTIONAL
        );

        assertEquals(EquivRefs.of(expectedEquivRefsMap), newEquivRefs);
    }

    @Test
    public void testCopyAndReplaceOutgoing() {
        EquivRefs newEquivRefs = equivRefs.copyAndReplaceOutgoing(ImmutableSet.of(ref2, ref4));
        Map<LookupRef, EquivDirection> expectedEquivRefsMap = ImmutableMap.of(
                ref1, INCOMING,
                ref2, OUTGOING,
                ref3, INCOMING,
                ref4, OUTGOING
        );
        assertEquals(EquivRefs.of(expectedEquivRefsMap), newEquivRefs);

        newEquivRefs = equivRefs.copyAndReplaceOutgoing(ImmutableSet.of(ref1, ref3));
        expectedEquivRefsMap = ImmutableMap.of(
                ref1, BIDIRECTIONAL,
                ref3, BIDIRECTIONAL
        );
        assertEquals(EquivRefs.of(expectedEquivRefsMap), newEquivRefs);
    }

    @Test
    public void testCopyAndReplaceIncoming() {
        EquivRefs newEquivRefs = equivRefs.copyAndReplaceIncoming(ImmutableSet.of(ref1, ref4));
        Map<LookupRef, EquivDirection> expectedEquivRefsMap = ImmutableMap.of(
                ref1, INCOMING,
                ref2, OUTGOING,
                ref3, OUTGOING,
                ref4, INCOMING
        );
        assertEquals(EquivRefs.of(expectedEquivRefsMap), newEquivRefs);

        newEquivRefs = equivRefs.copyAndReplaceIncoming(ImmutableSet.of(ref2, ref3));
        expectedEquivRefsMap = ImmutableMap.of(
                ref2, BIDIRECTIONAL,
                ref3, BIDIRECTIONAL
        );
        assertEquals(EquivRefs.of(expectedEquivRefsMap), newEquivRefs);
    }
}