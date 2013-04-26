package org.atlasapi.persistence.media.entity;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isOneOf;
import static org.junit.Assert.assertThat;

import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Identified;
import org.junit.Test;

import com.google.common.collect.Iterables;


public class IdentifiedTranslatorTest {
    
    private static final IdentifiedTranslator identifiedTranslator = new IdentifiedTranslator();

    @Test
    public void testEncodeAndDecode() {
        
        Identified identified = createIdentified();
        
        Identified decodedIdentified = identifiedTranslator.fromDBObject(identifiedTranslator.toDBObject(null, identified), null);
        
        assertThat(decodedIdentified.getAliases().size(), is(4));
        
        Alias existing = new Alias("namespace", "value");
        Alias canonicalAlias = new Alias("uri", "canonicalUri");
        Alias aliasUrl1 = new Alias("uri", "aliasUrl1");
        Alias aliasUrl2 = new Alias("uri", "aliasUrl2");
        
        assertThat(Iterables.get(decodedIdentified.getAliases(), 0), isOneOf(existing, canonicalAlias, aliasUrl1, aliasUrl2));
        assertThat(Iterables.get(decodedIdentified.getAliases(), 1), isOneOf(existing, canonicalAlias, aliasUrl1, aliasUrl2));
        assertThat(Iterables.get(decodedIdentified.getAliases(), 2), isOneOf(existing, canonicalAlias, aliasUrl1, aliasUrl2));
        assertThat(Iterables.get(decodedIdentified.getAliases(), 3), isOneOf(existing, canonicalAlias, aliasUrl1, aliasUrl2));
    }
    
    // Ensure that encoding and decoding multiple times doesn't add duplicate aliases
    @Test
    public void testDoubleEncodeAndDecode() {
        
        Identified identified = createIdentified();
        
        Identified decodedIdentified = identifiedTranslator.fromDBObject(identifiedTranslator.toDBObject(null, identifiedTranslator.fromDBObject(identifiedTranslator.toDBObject(null, identified), null)), null);
        
        assertThat(decodedIdentified.getAliases().size(), is(4));
        
        Alias existing = new Alias("namespace", "value");
        Alias canonicalAlias = new Alias("uri", "canonicalUri");
        Alias aliasUrl1 = new Alias("uri", "aliasUrl1");
        Alias aliasUrl2 = new Alias("uri", "aliasUrl2");
        
        assertThat(Iterables.get(decodedIdentified.getAliases(), 0), isOneOf(existing, canonicalAlias, aliasUrl1, aliasUrl2));
        assertThat(Iterables.get(decodedIdentified.getAliases(), 1), isOneOf(existing, canonicalAlias, aliasUrl1, aliasUrl2));
        assertThat(Iterables.get(decodedIdentified.getAliases(), 2), isOneOf(existing, canonicalAlias, aliasUrl1, aliasUrl2));
        assertThat(Iterables.get(decodedIdentified.getAliases(), 3), isOneOf(existing, canonicalAlias, aliasUrl1, aliasUrl2));
    }

    private Identified createIdentified() {
        Identified identified = new Identified();
        
        identified.setCanonicalUri("canonicalUri");
        identified.addAliasUrl("aliasUrl1");
        identified.addAliasUrl("aliasUrl2");
        
        identified.addAlias(new Alias("namespace", "value"));
        
        return identified;
    }

}
