package org.atlasapi.persistence.media.entity;

import java.util.List;
import java.util.Set;

import junit.framework.TestCase;

import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Identified;
import org.joda.time.DateTime;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.metabroadcast.common.time.DateTimeZones;
import com.mongodb.DBObject;

public class DescriptionTranslatorTest extends TestCase {
	
    private DateTime lastUpdated = new DateTime(DateTimeZones.UTC);

	@SuppressWarnings("unchecked")
    public void testShouldConvertFromDescription() throws Exception {
        Identified desc = new Identified();
        desc.setCanonicalUri("canonicalUri");
        desc.setCurie("curie");
        
        Set<String> aliases = Sets.newHashSet();
        aliases.add("alias1");
        aliases.add("alias2");
        desc.setAliasUrls(aliases);
        
        IdentifiedTranslator translator = new IdentifiedTranslator();
        DBObject dbObj = translator.toDBObject(null, desc);
        
        assertEquals("canonicalUri", dbObj.get(IdentifiedTranslator.ID));
        assertEquals("curie", dbObj.get("curie"));
        
        List<String> a = (List<String>) dbObj.get("aliases");
        assertEquals(2, a.size());
        for (String alias: a) {
            assertTrue(aliases.contains(alias));
        }
        
    }
    
    public void testShouldConvertToDescription() throws Exception {
        Identified desc = new Identified();
        desc.setCanonicalUri("canonicalUri");
        desc.setCurie("curie");
        
        Set<String> aliases = Sets.newHashSet();
        aliases.add("alias1");
        aliases.add("alias2");
        desc.setAliasUrls(aliases);
        
        IdentifiedTranslator translator = new IdentifiedTranslator();
        DBObject dbObj = translator.toDBObject(null, desc);
        Identified description = translator.fromDBObject(dbObj, null);
        
        assertEquals(desc.getCanonicalUri(), description.getCanonicalUri());
        assertEquals(desc.getCurie(), description.getCurie());
        Iterable<Alias> expectedAliases = Iterables.concat(
            desc.getAliases(), 
            ImmutableSet.of(new Alias("uri", "canonicalUri"), new Alias("uri", "alias1"), new Alias("uri", "alias2"))
            );
        assertEquals(expectedAliases, description.getAliases());
    }
    
    public void testShouldConvertToDescriptionByRef() throws Exception {
        Identified desc = new Identified();
        desc.setCanonicalUri("canonicalUri");
        desc.setCurie("curie");
        
        Set<String> aliases = Sets.newHashSet();
        aliases.add("alias1");
        aliases.add("alias2");
        desc.setAliasUrls(aliases);
        
        desc.setLastUpdated(lastUpdated);
        
        IdentifiedTranslator translator = new IdentifiedTranslator();
        DBObject dbObj = translator.toDBObject(null, desc);
        
        Identified description = new Identified();
        translator.fromDBObject(dbObj, description);
        
        assertEquals(desc.getCanonicalUri(), description.getCanonicalUri());
        assertEquals(desc.getCurie(), description.getCurie());
        assertEquals(desc.getAliasUrls(), description.getAliasUrls());
        assertEquals(desc.getLastUpdated(), description.getLastUpdated());
    }
}
