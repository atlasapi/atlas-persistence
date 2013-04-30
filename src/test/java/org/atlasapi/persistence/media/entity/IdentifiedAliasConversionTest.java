package org.atlasapi.persistence.media.entity;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.isOneOf;
import static org.junit.Assert.assertThat;

import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Identified;
import org.junit.Test;

import com.google.common.collect.Iterables;


public class IdentifiedAliasConversionTest {
    
    private static final IdentifiedTranslator identifiedTranslator = new IdentifiedTranslator(); 

    @Test
    public void testBBCProgrammePidConversion() {
        Identified identified = new Identified();
        
        identified.setCanonicalUri("http://www.bbc.co.uk/programmes/b01qcq2w");
        
        Identified decodedIdentified = encodeAndDecode(identified);
        
        assertThat(decodedIdentified.getAliases().size(), equalTo(2));
        
        Alias canonicalAlias = new Alias("uri", "http://www.bbc.co.uk/programmes/b01qcq2w");
        Alias pidAlias = new Alias("gb:bbc:pid", "b01qcq2w");
        
        assertThat(Iterables.get(decodedIdentified.getAliases(), 0), isOneOf(canonicalAlias, pidAlias));
        assertThat(Iterables.get(decodedIdentified.getAliases(), 1), isOneOf(canonicalAlias, pidAlias));
    }

    @Test
    public void testBBCWSArchiveEpisodePidConversion() {
        Identified identified = new Identified();
        
        identified.setCanonicalUri("http://wsarchive.bbc.co.uk/episodes/12345");
        
        Identified decodedIdentified = encodeAndDecode(identified);
        
        assertThat(decodedIdentified.getAliases().size(), equalTo(2));
        
        Alias canonicalAlias = new Alias("uri", "http://wsarchive.bbc.co.uk/episodes/12345");
        Alias pidAlias = new Alias("gb:bbc:pid", "12345");
        
        assertThat(Iterables.get(decodedIdentified.getAliases(), 0), isOneOf(canonicalAlias, pidAlias));
        assertThat(Iterables.get(decodedIdentified.getAliases(), 1), isOneOf(canonicalAlias, pidAlias));
    }

    @Test
    public void testBBCWSArchiveBrandPidConversion() {
        Identified identified = new Identified();
        
        identified.setCanonicalUri("http://wsarchive.bbc.co.uk/brands/12345");
        
        Identified decodedIdentified = encodeAndDecode(identified);
        
        assertThat(decodedIdentified.getAliases().size(), equalTo(2));
        
        Alias canonicalAlias = new Alias("uri", "http://wsarchive.bbc.co.uk/brands/12345");
        Alias pidAlias = new Alias("gb:bbc:pid", "12345");
        
        assertThat(Iterables.get(decodedIdentified.getAliases(), 0), isOneOf(canonicalAlias, pidAlias));
        assertThat(Iterables.get(decodedIdentified.getAliases(), 1), isOneOf(canonicalAlias, pidAlias));
    }

    @Test
    public void testBBCIPidConversion() {
        Identified identified = new Identified();
        
        identified.setCanonicalUri("http://bbc.co.uk/i/p016n90j/");
        
        Identified decodedIdentified = encodeAndDecode(identified);
        
        assertThat(decodedIdentified.getAliases().size(), equalTo(2));
        
        Alias canonicalAlias = new Alias("uri", "http://bbc.co.uk/i/p016n90j/");
        Alias pidAlias = new Alias("gb:bbc:pid", "p016n90j");
        
        assertThat(Iterables.get(decodedIdentified.getAliases(), 0), isOneOf(canonicalAlias, pidAlias));
        assertThat(Iterables.get(decodedIdentified.getAliases(), 1), isOneOf(canonicalAlias, pidAlias));
    }

    @Test
    public void testBBCIPlayerEpisodePidConversion() {
        Identified identified = new Identified();
        
        identified.setCanonicalUri("http://www.bbc.co.uk/iplayer/episode/p00vm0r1");
        
        Identified decodedIdentified = encodeAndDecode(identified);
        
        assertThat(decodedIdentified.getAliases().size(), equalTo(2));
        
        Alias canonicalAlias = new Alias("uri", "http://www.bbc.co.uk/iplayer/episode/p00vm0r1");
        Alias pidAlias = new Alias("gb:bbc:pid", "p00vm0r1");
        
        assertThat(Iterables.get(decodedIdentified.getAliases(), 0), isOneOf(canonicalAlias, pidAlias));
        assertThat(Iterables.get(decodedIdentified.getAliases(), 1), isOneOf(canonicalAlias, pidAlias));
    }

    @Test
    public void testBBCReduxServiceIdConversion() {
        Identified identified = new Identified();
        
        identified.setCanonicalUri("http://devapi.bbcredux.com/channels/r5lsx");
        
        Identified decodedIdentified = encodeAndDecode(identified);
        
        assertThat(decodedIdentified.getAliases().size(), equalTo(2));
        
        Alias canonicalAlias = new Alias("uri", "http://devapi.bbcredux.com/channels/r5lsx");
        Alias pidAlias = new Alias("gb:bbcredux:service-id", "r5lsx");
        
        assertThat(Iterables.get(decodedIdentified.getAliases(), 0), isOneOf(canonicalAlias, pidAlias));
        assertThat(Iterables.get(decodedIdentified.getAliases(), 1), isOneOf(canonicalAlias, pidAlias));
    }
    
    @Test
    public void testPaBrandIdConversion() {
        Identified identified = new Identified();
        
        identified.setCanonicalUri("http://pressassociation.com/brands/87");
        
        Identified decodedIdentified = encodeAndDecode(identified);
        
        assertThat(decodedIdentified.getAliases().size(), equalTo(2));
        
        Alias canonicalAlias = new Alias("uri", "http://pressassociation.com/brands/87");
        Alias pidAlias = new Alias("gb:pa:series-id", "87");
        
        assertThat(Iterables.get(decodedIdentified.getAliases(), 0), isOneOf(canonicalAlias, pidAlias));
        assertThat(Iterables.get(decodedIdentified.getAliases(), 1), isOneOf(canonicalAlias, pidAlias));
    }
    
    @Test
    public void testPaSeriesIdConversion() {
        Identified identified = new Identified();
        
        identified.setCanonicalUri("http://pressassociation.com/series/87");
        
        Identified decodedIdentified = encodeAndDecode(identified);
        
        assertThat(decodedIdentified.getAliases().size(), equalTo(2));
        
        Alias canonicalAlias = new Alias("uri", "http://pressassociation.com/series/87");
        Alias pidAlias = new Alias("gb:mbst:pa:season-id", "87");
        
        assertThat(Iterables.get(decodedIdentified.getAliases(), 0), isOneOf(canonicalAlias, pidAlias));
        assertThat(Iterables.get(decodedIdentified.getAliases(), 1), isOneOf(canonicalAlias, pidAlias));
    }
    
    @Test
    public void testPaEpisodeIdConversion() {
        Identified identified = new Identified();
        
        identified.setCanonicalUri("http://pressassociation.com/episodes/87");
        
        Identified decodedIdentified = encodeAndDecode(identified);
        
        assertThat(decodedIdentified.getAliases().size(), equalTo(2));
        
        Alias canonicalAlias = new Alias("uri", "http://pressassociation.com/episodes/87");
        Alias pidAlias = new Alias("gb:pa:prog-id", "87");
        
        assertThat(Iterables.get(decodedIdentified.getAliases(), 0), isOneOf(canonicalAlias, pidAlias));
        assertThat(Iterables.get(decodedIdentified.getAliases(), 1), isOneOf(canonicalAlias, pidAlias));
    }
    
    @Test
    public void testPaFilmIdConversion() {
        Identified identified = new Identified();
        
        identified.setCanonicalUri("http://pressassociation.com/films/87");
        
        Identified decodedIdentified = encodeAndDecode(identified);
        
        assertThat(decodedIdentified.getAliases().size(), equalTo(2));
        
        Alias canonicalAlias = new Alias("uri", "http://pressassociation.com/films/87");
        Alias pidAlias = new Alias("gb:pa:prog-id", "87");
        
        assertThat(Iterables.get(decodedIdentified.getAliases(), 0), isOneOf(canonicalAlias, pidAlias));
        assertThat(Iterables.get(decodedIdentified.getAliases(), 1), isOneOf(canonicalAlias, pidAlias));
    }

    @Test
    public void testPaChannelIdConversion() {
        Identified identified = new Identified();
        
        identified.setCanonicalUri("http://pressassociation.com/channels/87");
        
        Identified decodedIdentified = encodeAndDecode(identified);
        
        assertThat(decodedIdentified.getAliases().size(), equalTo(2));
        
        Alias canonicalAlias = new Alias("uri", "http://pressassociation.com/channels/87");
        Alias pidAlias = new Alias("gb:pa:channel", "87");
        
        assertThat(Iterables.get(decodedIdentified.getAliases(), 0), isOneOf(canonicalAlias, pidAlias));
        assertThat(Iterables.get(decodedIdentified.getAliases(), 1), isOneOf(canonicalAlias, pidAlias));
    }

    @Test
    public void testPaStationIdConversion() {
        Identified identified = new Identified();
        
        identified.setCanonicalUri("http://pressassociation.com/stations/87");
        
        Identified decodedIdentified = encodeAndDecode(identified);
        
        assertThat(decodedIdentified.getAliases().size(), equalTo(2));
        
        Alias canonicalAlias = new Alias("uri", "http://pressassociation.com/stations/87");
        Alias pidAlias = new Alias("gb:pa:station", "87");
        
        assertThat(Iterables.get(decodedIdentified.getAliases(), 0), isOneOf(canonicalAlias, pidAlias));
        assertThat(Iterables.get(decodedIdentified.getAliases(), 1), isOneOf(canonicalAlias, pidAlias));
    }

    @Test
    public void testPaRegionIdConversion() {
        Identified identified = new Identified();
        
        identified.setCanonicalUri("http://pressassociation.com/regions/87");
        
        Identified decodedIdentified = encodeAndDecode(identified);
        
        assertThat(decodedIdentified.getAliases().size(), equalTo(2));
        
        Alias canonicalAlias = new Alias("uri", "http://pressassociation.com/regions/87");
        Alias pidAlias = new Alias("gb:pa:region", "87");
        
        assertThat(Iterables.get(decodedIdentified.getAliases(), 0), isOneOf(canonicalAlias, pidAlias));
        assertThat(Iterables.get(decodedIdentified.getAliases(), 1), isOneOf(canonicalAlias, pidAlias));
    }

    @Test
    public void testPaPlatformIdConversion() {
        Identified identified = new Identified();
        
        identified.setCanonicalUri("http://pressassociation.com/platforms/87");
        
        Identified decodedIdentified = encodeAndDecode(identified);
        
        assertThat(decodedIdentified.getAliases().size(), equalTo(2));
        
        Alias canonicalAlias = new Alias("uri", "http://pressassociation.com/platforms/87");
        Alias pidAlias = new Alias("gb:pa:platform", "87");
        
        assertThat(Iterables.get(decodedIdentified.getAliases(), 0), isOneOf(canonicalAlias, pidAlias));
        assertThat(Iterables.get(decodedIdentified.getAliases(), 1), isOneOf(canonicalAlias, pidAlias));
    }

    @Test
    public void testYouViewServiceIdConversion() {
        Identified identified = new Identified();
        
        identified.setCanonicalUri("http://youview.com/service/56251");
        
        Identified decodedIdentified = encodeAndDecode(identified);
        
        assertThat(decodedIdentified.getAliases().size(), equalTo(2));
        
        Alias canonicalAlias = new Alias("uri", "http://youview.com/service/56251");
        Alias pidAlias = new Alias("gb:youview:service-id", "56251");
        
        assertThat(Iterables.get(decodedIdentified.getAliases(), 0), isOneOf(canonicalAlias, pidAlias));
        assertThat(Iterables.get(decodedIdentified.getAliases(), 1), isOneOf(canonicalAlias, pidAlias));
    }

    @Test
    public void testYouViewProgrammeIdConversion() {
        Identified identified = new Identified();
        
        identified.setCanonicalUri("http://youview.com/programme/56251");
        
        Identified decodedIdentified = encodeAndDecode(identified);
        
        assertThat(decodedIdentified.getAliases().size(), equalTo(2));
        
        Alias canonicalAlias = new Alias("uri", "http://youview.com/programme/56251");
        Alias pidAlias = new Alias("gb:youview:programme-id", "56251");
        
        assertThat(Iterables.get(decodedIdentified.getAliases(), 0), isOneOf(canonicalAlias, pidAlias));
        assertThat(Iterables.get(decodedIdentified.getAliases(), 1), isOneOf(canonicalAlias, pidAlias));
    }

    @Test
    public void testYouViewScheduleeventIdConversion() {
        Identified identified = new Identified();
        
        identified.setCanonicalUri("http://youview.com/scheduleevent/56251");
        
        Identified decodedIdentified = encodeAndDecode(identified);
        
        assertThat(decodedIdentified.getAliases().size(), equalTo(2));
        
        Alias canonicalAlias = new Alias("uri", "http://youview.com/scheduleevent/56251");
        Alias pidAlias = new Alias("gb:youview:scheduleevent-id", "56251");
        
        assertThat(Iterables.get(decodedIdentified.getAliases(), 0), isOneOf(canonicalAlias, pidAlias));
        assertThat(Iterables.get(decodedIdentified.getAliases(), 1), isOneOf(canonicalAlias, pidAlias));
    }

    @Test
    public void testYouViewScheduleeventAliasConversion() {
        Identified identified = new Identified();
        identified.addAlias(new Alias("youview:scheduleevent", "8754502"));
        
        Identified decodedIdentified = encodeAndDecode(identified);
        
        Alias scheduleEventAlias = new Alias("gb:youview:scheduleevent-id", "8754502");
        
        assertThat(Iterables.getOnlyElement(decodedIdentified.getAliases()), isOneOf(scheduleEventAlias));
    }

    @Test
    public void testCridAliasConversion() {
        Identified identified = new Identified();
        identified.addAlias(new Alias("dvb:pcrid", "crid://www.itv.com/49837472"));
        identified.addAlias(new Alias("dvb:scrid", "crid://www.five.tv/R53A"));
        
        Identified decodedIdentified = encodeAndDecode(identified);
        
        assertThat(decodedIdentified.getAliases().size(), equalTo(2));
        
        Alias pcridAlias = new Alias("gb:dvb:pcrid", "crid://www.itv.com/49837472");
        Alias scridAlias = new Alias("gb:dvb:scrid", "crid://www.five.tv/R53A");
        
        assertThat(Iterables.get(decodedIdentified.getAliases(), 0), isOneOf(pcridAlias, scridAlias));
        assertThat(Iterables.get(decodedIdentified.getAliases(), 1), isOneOf(pcridAlias, scridAlias));
    }

    @Test
    public void testDvbEventLocatorConversion() {
        Identified identified = new Identified();
        
        identified.setCanonicalUri("dvb://2ks..432sa;a23t");
        
        Identified decodedIdentified = encodeAndDecode(identified);
        
        assertThat(decodedIdentified.getAliases().size(), equalTo(2));
        
        Alias canonicalAlias = new Alias("uri", "dvb://2ks..432sa;a23t");
        Alias pidAlias = new Alias("gb:dvb:event-locator", "dvb://2ks..432sa;a23t");
        
        assertThat(Iterables.get(decodedIdentified.getAliases(), 0), isOneOf(canonicalAlias, pidAlias));
        assertThat(Iterables.get(decodedIdentified.getAliases(), 1), isOneOf(canonicalAlias, pidAlias));
    }

    @Test
    public void testXmlTvChannelIdConversion() {
        Identified identified = new Identified();
        
        identified.setCanonicalUri("http://xmltv.radiotimes.com/channels/2668");
        
        Identified decodedIdentified = encodeAndDecode(identified);
        
        assertThat(decodedIdentified.getAliases().size(), equalTo(2));
        
        Alias canonicalAlias = new Alias("uri", "http://xmltv.radiotimes.com/channels/2668");
        Alias pidAlias = new Alias("gb:mbst:xmltv:channel-id", "2668");
        
        assertThat(Iterables.get(decodedIdentified.getAliases(), 0), isOneOf(canonicalAlias, pidAlias));
        assertThat(Iterables.get(decodedIdentified.getAliases(), 1), isOneOf(canonicalAlias, pidAlias));
    }

    @Test
    public void testLoveFilmEpisodeSkuConversion() {
        Identified identified = new Identified();
        
        identified.setCanonicalUri("http://lovefilm.com/episodes/2668");
        
        Identified decodedIdentified = encodeAndDecode(identified);
        
        assertThat(decodedIdentified.getAliases().size(), equalTo(2));
        
        Alias canonicalAlias = new Alias("uri", "http://lovefilm.com/episodes/2668");
        Alias pidAlias = new Alias("gb:lovefilm:sku", "2668");
        
        assertThat(Iterables.get(decodedIdentified.getAliases(), 0), isOneOf(canonicalAlias, pidAlias));
        assertThat(Iterables.get(decodedIdentified.getAliases(), 1), isOneOf(canonicalAlias, pidAlias));
    }

    @Test
    public void testLoveFilmFilmSkuConversion() {
        Identified identified = new Identified();
        
        identified.setCanonicalUri("http://lovefilm.com/films/2668");
        
        Identified decodedIdentified = encodeAndDecode(identified);
        
        assertThat(decodedIdentified.getAliases().size(), equalTo(2));
        
        Alias canonicalAlias = new Alias("uri", "http://lovefilm.com/films/2668");
        Alias pidAlias = new Alias("gb:lovefilm:sku", "2668");
        
        assertThat(Iterables.get(decodedIdentified.getAliases(), 0), isOneOf(canonicalAlias, pidAlias));
        assertThat(Iterables.get(decodedIdentified.getAliases(), 1), isOneOf(canonicalAlias, pidAlias));
    }

    @Test
    public void testLoveFilmSeriesSkuConversion() {
        Identified identified = new Identified();
        
        identified.setCanonicalUri("http://lovefilm.com/seasons/2668");
        
        Identified decodedIdentified = encodeAndDecode(identified);
        
        assertThat(decodedIdentified.getAliases().size(), equalTo(2));
        
        Alias canonicalAlias = new Alias("uri", "http://lovefilm.com/seasons/2668");
        Alias pidAlias = new Alias("gb:lovefilm:sku", "2668");
        
        assertThat(Iterables.get(decodedIdentified.getAliases(), 0), isOneOf(canonicalAlias, pidAlias));
        assertThat(Iterables.get(decodedIdentified.getAliases(), 1), isOneOf(canonicalAlias, pidAlias));
    }

    @Test
    public void testLoveFilmBrandSkuConversion() {
        Identified identified = new Identified();
        
        identified.setCanonicalUri("http://lovefilm.com/shows/2668");
        
        Identified decodedIdentified = encodeAndDecode(identified);
        
        assertThat(decodedIdentified.getAliases().size(), equalTo(2));
        
        Alias canonicalAlias = new Alias("uri", "http://lovefilm.com/shows/2668");
        Alias pidAlias = new Alias("gb:lovefilm:sku", "2668");
        
        assertThat(Iterables.get(decodedIdentified.getAliases(), 0), isOneOf(canonicalAlias, pidAlias));
        assertThat(Iterables.get(decodedIdentified.getAliases(), 1), isOneOf(canonicalAlias, pidAlias));
    }

    @Test
    public void testImdbIdConversion() {
        Identified identified = new Identified();
        
        identified.setCanonicalUri("http://imdb.com/title/tt170461");
        
        Identified decodedIdentified = encodeAndDecode(identified);
        
        assertThat(decodedIdentified.getAliases().size(), equalTo(2));
        
        Alias canonicalAlias = new Alias("uri", "http://imdb.com/title/tt170461");
        Alias pidAlias = new Alias("zz:imdb:id", "tt170461");
        
        assertThat(Iterables.get(decodedIdentified.getAliases(), 0), isOneOf(canonicalAlias, pidAlias));
        assertThat(Iterables.get(decodedIdentified.getAliases(), 1), isOneOf(canonicalAlias, pidAlias));
    }
    
    public Identified encodeAndDecode(Identified identified) {
        return identifiedTranslator.fromDBObject(identifiedTranslator.toDBObject(null, identified), null);
    }

}
