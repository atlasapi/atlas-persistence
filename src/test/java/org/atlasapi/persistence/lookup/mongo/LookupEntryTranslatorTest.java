package org.atlasapi.persistence.lookup.mongo;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.metabroadcast.common.time.DateTimeZones;
import com.mongodb.DBObject;
import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentCategory;
import org.atlasapi.persistence.lookup.entry.EquivRefs;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.joda.time.DateTime;
import org.junit.Test;

import java.util.Set;

import static org.atlasapi.persistence.lookup.entry.EquivRefs.EquivDirection.OUTGOING;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;


public class LookupEntryTranslatorTest {

    private static final String ACTIVELY_PUBLISHED = "activelyPublished";
    private final LookupEntryTranslator translator
        = new LookupEntryTranslator();
    
    @Test
    public void testTranslatingALookupEntry() {
        
        LookupRef self
            = ref("uri", 1L, Publisher.BBC, ContentCategory.CHILD_ITEM);
        Set<String> aliasUris = ImmutableSet.of("uri","alias");
        Set<Alias> aliases = ImmutableSet.of(new Alias("ns","val"));
        Set<LookupRef> directEquivs = ImmutableSet.of(
            ref("uri", 1L, Publisher.BBC, ContentCategory.CHILD_ITEM), 
            ref("dir",2L, Publisher.PA, ContentCategory.CHILD_ITEM));
        Set<LookupRef> explicit = ImmutableSet.of(
            ref("uri", 1L, Publisher.BBC, ContentCategory.CHILD_ITEM),
            ref("exp",3L, Publisher.BT, ContentCategory.CHILD_ITEM));
        Set<LookupRef> equivs = ImmutableSet.of(
            ref("uri", 1L, Publisher.BBC, ContentCategory.CHILD_ITEM), 
            ref("dir",2L, Publisher.PA, ContentCategory.CHILD_ITEM),
            ref("exp",3L, Publisher.BT, ContentCategory.CHILD_ITEM));
        DateTime created = DateTime.now(DateTimeZones.UTC);
        DateTime updated = DateTime.now(DateTimeZones.UTC);

        LookupEntry e = new LookupEntry(
                "uri",
                1L,
                self,
                aliasUris,
                aliases,
                EquivRefs.of(directEquivs, OUTGOING),
                EquivRefs.of(explicit, OUTGOING),
                EquivRefs.of(),
                equivs,
                created,
                updated,
                updated,
                true
        );
        
        DBObject dbo = translator.toDbo(e);
        
        // not stored if true, since this is the common case, and we don't want 
        // to write all records unnecessarily when this field has been added
        assertFalse(dbo.containsField(ACTIVELY_PUBLISHED));
        
        LookupEntry t = translator.fromDbo(dbo);
        
        assertEquals(e.uri(), t.uri());
        assertEquals(e.id(), t.id());
        assertEquals(e.lookupRef(), t.lookupRef());
        assertEquals(e.aliasUrls(), t.aliasUrls());
        assertEquals(e.aliases(), t.aliases());
        assertEquals(e.getDirectEquivalents().getLookupRefs(), t.getDirectEquivalents().getLookupRefs());
        assertEquals(e.getExplicitEquivalents().getLookupRefs(), t.getExplicitEquivalents().getLookupRefs());
        assertEquals(e.equivalents(), t.equivalents());
        assertEquals(e.created(), t.created());
        assertEquals(e.updated(), t.updated());
        assertEquals(e.activelyPublished(), t.activelyPublished());
        
        e = new LookupEntry(
                "uri",
                1L,
                self,
                aliasUris,
                aliases,
                EquivRefs.of(directEquivs, OUTGOING),
                EquivRefs.of(explicit, OUTGOING),
                EquivRefs.of(),
                equivs,
                created,
                updated,
                updated,
                false
        );


        dbo = translator.toDbo(e);
        
        assertTrue(dbo.containsField(ACTIVELY_PUBLISHED));
        
        t = translator.fromDbo(dbo);
        assertEquals(e.activelyPublished(), t.activelyPublished());
        
    }
    
    @Test
    public void testEntryListsContainSelfRef() {
        
        LookupRef self
            = ref("uri", 1L, Publisher.BBC, ContentCategory.CHILD_ITEM);
        Set<String> aliasUris = ImmutableSet.of("uri","alias");
        Set<Alias> aliases = ImmutableSet.of(new Alias("ns","val"));
        Set<LookupRef> directEquivs = ImmutableSet.of(
            ref("uri", 1L, Publisher.BBC, ContentCategory.CHILD_ITEM), 
            ref("dir",2L, Publisher.PA, ContentCategory.CHILD_ITEM));
        Set<LookupRef> explicit = ImmutableSet.of(
            ref("uri", 1L, Publisher.BBC, ContentCategory.CHILD_ITEM),
            ref("exp",3L, Publisher.BT, ContentCategory.CHILD_ITEM));
        Set<LookupRef> equivs = ImmutableSet.of(
            ref("uri", 1L, Publisher.BBC, ContentCategory.CHILD_ITEM), 
            ref("dir",2L, Publisher.PA, ContentCategory.CHILD_ITEM),
            ref("exp",3L, Publisher.BT, ContentCategory.CHILD_ITEM));
        DateTime created = DateTime.now(DateTimeZones.UTC);
        DateTime updated = DateTime.now(DateTimeZones.UTC);

        LookupEntry e = new LookupEntry(
                "uri",
                1L,
                self,
                aliasUris,
                aliases,
                EquivRefs.of(directEquivs, OUTGOING),
                EquivRefs.of(explicit, OUTGOING),
                EquivRefs.of(),
                equivs,
                created,
                updated,
                updated,
                true
        );
        
        DBObject dbo = translator.toDbo(e);
        
        LookupEntry t = translator.fromDbo(dbo);
        
        LookupRef selfRef = t.lookupRef();
        containsInstance(selfRef, t.getDirectEquivalents().getLookupRefs());
        containsInstance(selfRef, t.getExplicitEquivalents().getLookupRefs());
        containsInstance(selfRef, t.equivalents());
        
    }

    private void containsInstance(LookupRef ref, Set<LookupRef> refs) {
        assertTrue(refs.contains(ref));
        refs = Sets.newHashSet(refs);
        refs.retainAll(ImmutableSet.of(ref));
        assertThat(Iterables.getOnlyElement(refs), sameInstance(ref));
    }


    private LookupRef ref(String uri, long id, Publisher src, ContentCategory cat) {
        return new LookupRef(uri, id, src, cat);
    }

}
