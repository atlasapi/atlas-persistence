package org.atlasapi.persistence.lookup.mongo;

import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.Set;

import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentCategory;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.joda.time.DateTime;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.metabroadcast.common.time.DateTimeZones;
import com.mongodb.DBObject;


public class LookupEntryTranslatorTest {

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
        
        LookupEntry e = new LookupEntry("uri", 1L, self, aliasUris, aliases, directEquivs, explicit, equivs, created, updated);
        
        DBObject dbo = translator.toDbo(e);
        
        LookupEntry t = translator.fromDbo(dbo);
        
        assertEquals(e.uri(), t.uri());
        assertEquals(e.id(), t.id());
        assertEquals(e.lookupRef(), t.lookupRef());
        assertEquals(e.aliasUrls(), t.aliasUrls());
        assertEquals(e.aliases(), t.aliases());
        assertEquals(e.directEquivalents(), t.directEquivalents());
        assertEquals(e.explicitEquivalents(), t.explicitEquivalents());
        assertEquals(e.equivalents(), t.equivalents());
        assertEquals(e.created(), t.created());
        assertEquals(e.updated(), t.updated());
        
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
        
        LookupEntry e = new LookupEntry("uri", 1L, self, aliasUris, aliases, directEquivs, explicit, equivs, created, updated);
        
        DBObject dbo = translator.toDbo(e);
        
        LookupEntry t = translator.fromDbo(dbo);
        
        LookupRef selfRef = t.lookupRef();
        containsInstance(selfRef, t.directEquivalents());
        containsInstance(selfRef, t.explicitEquivalents());
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
