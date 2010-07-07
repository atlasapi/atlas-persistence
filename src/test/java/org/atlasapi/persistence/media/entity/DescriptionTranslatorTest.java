package org.atlasapi.persistence.media.entity;

import java.util.List;
import java.util.Set;

import junit.framework.TestCase;

import org.atlasapi.media.entity.Description;
import org.atlasapi.persistence.media.entity.DescriptionTranslator;

import com.google.common.collect.Sets;
import com.mongodb.DBObject;

public class DescriptionTranslatorTest extends TestCase {
    @SuppressWarnings("unchecked")
    public void testShouldConvertFromDescription() throws Exception {
        Description desc = new Description();
        desc.setCanonicalUri("canonicalUri");
        desc.setCurie("curie");
        
        Set<String> aliases = Sets.newHashSet();
        aliases.add("alias1");
        aliases.add("alias2");
        desc.setAliases(aliases);
        
        DescriptionTranslator translator = new DescriptionTranslator();
        DBObject dbObj = translator.toDBObject(null, desc);
        
        assertEquals("canonicalUri", dbObj.get(DescriptionTranslator.CANONICAL_URI));
        assertEquals("curie", dbObj.get("curie"));
        
        List<String> a = (List<String>) dbObj.get("aliases");
        assertEquals(2, a.size());
        for (String alias: a) {
            assertTrue(aliases.contains(alias));
        }
    }
    
    public void testShouldConvertToDescription() throws Exception {
        Description desc = new Description();
        desc.setCanonicalUri("canonicalUri");
        desc.setCurie("curie");
        
        Set<String> aliases = Sets.newHashSet();
        aliases.add("alias1");
        aliases.add("alias2");
        desc.setAliases(aliases);
        
        DescriptionTranslator translator = new DescriptionTranslator();
        DBObject dbObj = translator.toDBObject(null, desc);
        Description description = translator.fromDBObject(dbObj, null);
        
        assertEquals(desc.getCanonicalUri(), description.getCanonicalUri());
        assertEquals(desc.getCurie(), description.getCurie());
        assertEquals(desc.getAliases(), description.getAliases());
    }
    
    public void testShouldConvertToDescriptionByRef() throws Exception {
        Description desc = new Description();
        desc.setCanonicalUri("canonicalUri");
        desc.setCurie("curie");
        
        Set<String> aliases = Sets.newHashSet();
        aliases.add("alias1");
        aliases.add("alias2");
        desc.setAliases(aliases);
        
        DescriptionTranslator translator = new DescriptionTranslator();
        DBObject dbObj = translator.toDBObject(null, desc);
        
        Description description = new Description();
        translator.fromDBObject(dbObj, description);
        
        assertEquals(desc.getCanonicalUri(), description.getCanonicalUri());
        assertEquals(desc.getCurie(), description.getCurie());
        assertEquals(desc.getAliases(), description.getAliases());
    }
}
