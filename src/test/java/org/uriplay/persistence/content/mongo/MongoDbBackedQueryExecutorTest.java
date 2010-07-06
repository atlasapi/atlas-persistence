/* Copyright 2009 Meta Broadcast Ltd

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License. You may
obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License. */

package org.uriplay.persistence.content.mongo;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.uriplay.content.criteria.ContentQueryBuilder.query;
import static org.uriplay.content.criteria.attribute.Attributes.BROADCAST_TRANSMISSION_END_TIME;
import static org.uriplay.content.criteria.attribute.Attributes.BROADCAST_TRANSMISSION_TIME;
import static org.uriplay.content.criteria.attribute.Attributes.EPISODE_POSITION;
import static org.uriplay.content.criteria.attribute.Attributes.ITEM_GENRE;
import static org.uriplay.content.criteria.attribute.Attributes.ITEM_PUBLISHER;
import static org.uriplay.content.criteria.attribute.Attributes.ITEM_TITLE;
import static org.uriplay.content.criteria.attribute.Attributes.LOCATION_TRANSPORT_TYPE;
import static org.uriplay.content.criteria.attribute.Attributes.VERSION_DURATION;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import junit.framework.TestCase;

import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.uriplay.content.criteria.ContentQueryBuilder;
import org.uriplay.content.criteria.attribute.Attributes;
import org.uriplay.media.TransportType;
import org.uriplay.media.entity.Brand;
import org.uriplay.media.entity.Broadcast;
import org.uriplay.media.entity.Description;
import org.uriplay.media.entity.Item;
import org.uriplay.media.entity.Playlist;
import org.uriplay.media.entity.Version;
import org.uriplay.persistence.testing.DummyContentData;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.metabroadcast.common.persistence.MongoTestHelper;
import com.metabroadcast.common.query.Selection;
import com.metabroadcast.common.time.DateTimeZones;
import com.mongodb.Mongo;

public class MongoDbBackedQueryExecutorTest extends TestCase {
	
    private DummyContentData data = new DummyContentData();

    private MongoDbBackedContentStore store;
    private MongoDBQueryExecutor queryExecutor;
    

    @Override
    protected void setUp() throws Exception {
    	super.setUp();
    	Mongo mongo = MongoTestHelper.anEmptyMongo();
    	
    	store = new MongoDbBackedContentStore(mongo, "uriplay");
    	queryExecutor = new MongoDBQueryExecutor(new MongoRoughSearch(store));
    	
    	store.createOrUpdatePlaylist(data.eastenders, true);
    	store.createOrUpdatePlaylist(data.apprentice, true);
    	store.createOrUpdatePlaylist(data.newsNight, true);
    	store.createOrUpdatePlaylist(data.ER, true);
    	
    	store.createOrUpdateItem(data.englishForCats);
    	store.createOrUpdateItem(data.eggsForBreakfast);
    	store.createOrUpdateItem(data.everyoneNeedsAnEel);
    	
    	store.createOrUpdatePlaylist(data.goodEastendersEpisodes, false);
    	store.createOrUpdatePlaylist(data.mentionedOnTwitter, false);
    }

    public void testFindingItemsByUri() throws Exception {
		checkItemQuery(query().equalTo(Attributes.ITEM_URI, data.englishForCats.getCanonicalUri()), data.englishForCats);
		
		// NewsNight is not an item, so this query should match no items
		checkItemQueryMatchesNothing(query().equalTo(Attributes.ITEM_URI, data.newsNight.getCanonicalUri()));
		
		// check an alias
		checkItemQuery(query().equalTo(Attributes.ITEM_URI, "http://dot.cotton"), data.dotCottonsBigAdventure);
	}
    
    public void testThatIfAnItemIsFoundItIsNotFilteredOut() throws Exception {
		ContentQueryBuilder matchesNoSubElements = query().equalTo(Attributes.ITEM_URI, data.englishForCats.getCanonicalUri()).equalTo(Attributes.LOCATION_URI, "nothing here");
		Item item = Iterables.getOnlyElement(queryExecutor.executeItemQuery(matchesNoSubElements.build()));
		assertThat(item.getCanonicalUri(), is(data.englishForCats.getCanonicalUri()));
		assertThat(item.getVersions(), is(Collections.<Version>emptySet()));
	}
	
	public void testFindingBrandsByUri() throws Exception {
		checkBrandQuery(query().equalTo(Attributes.BRAND_URI, data.eastenders.getCanonicalUri()), data.eastenders);

		// Eastenders is not an item
		checkBrandQueryMatchesNothing(query().equalTo(Attributes.ITEM_URI, data.eastenders.getCanonicalUri()));

		// Eastenders shoudn't be treated like a playlist
		checkBrandQueryMatchesNothing(query().equalTo(Attributes.PLAYLIST_URI, data.eastenders.getCanonicalUri()));
	
		// check an alias
		checkBrandQuery(query().equalTo(Attributes.BRAND_URI, "http://eastenders.bbc"), data.eastenders);
	}
	
	public void testFindingMultipleBrandsByUriReturnsTheResultsInTheRightOrder() throws Exception {
		
		List<Brand> brands = queryExecutor.executeBrandQuery(query().equalTo(Attributes.BRAND_URI, data.eastenders.getCanonicalUri(), data.apprentice.getCanonicalUri()).build());
		
		assertThat(brands, is(Arrays.asList(data.eastenders, data.apprentice)));
		
		brands = queryExecutor.executeBrandQuery(query().equalTo(Attributes.BRAND_URI, data.apprentice.getCanonicalUri(), data.eastenders.getCanonicalUri()).build());
		assertThat(brands, is(Arrays.asList(data.apprentice, data.eastenders)));

		
	}
	
	public void testFindingBrandsItemsWithinThem() throws Exception {
		checkBrandQuery(query().equalTo(Attributes.ITEM_URI, data.dotCottonsBigAdventure.getCanonicalUri()), data.eastenders);
	}
	
	public void testSelections() throws Exception {
		checkBrandQuery(query().equalTo(Attributes.BRAND_PUBLISHER, "bbc.co.uk").withSelection(new Selection(0, 3)), data.eastenders, data.apprentice, data.newsNight);
		checkBrandQuery(query().equalTo(Attributes.BRAND_PUBLISHER, "bbc.co.uk").withSelection(new Selection(1, 3)), data.apprentice, data.newsNight);
		checkBrandQuery(query().equalTo(Attributes.BRAND_PUBLISHER, "bbc.co.uk").withSelection(new Selection(0, 1)), data.eastenders);
		checkBrandQuery(query().equalTo(Attributes.BRAND_PUBLISHER, "bbc.co.uk").withSelection(new Selection(1, 2)), data.apprentice, data.newsNight);

		checkItemQuery(query().equalTo(Attributes.BRAND_URI, data.eastenders.getCanonicalUri()).withSelection(new Selection(0, 1)), data.dotCottonsBigAdventure);
		checkBrandQuery(query().equalTo(Attributes.PLAYLIST_URI, data.mentionedOnTwitter.getCanonicalUri()).withSelection(new Selection(0, 1)), data.eastenders);
	}
	
	public void testThatIfAnBrandIsFoundByUriOrCurieItIsNotFilteredOut() throws Exception {
		ContentQueryBuilder matchesNoSubElements = query().equalTo(Attributes.BRAND_URI, data.eastenders.getCanonicalUri()).equalTo(Attributes.LOCATION_URI, "nothing here");
		Brand brand = Iterables.getOnlyElement(queryExecutor.executeBrandQuery(matchesNoSubElements.build()));
		assertThat(brand.getCanonicalUri(), is(data.eastenders.getCanonicalUri()));
		assertThat(brand.getItems(), is(Collections.<Item>emptyList()));
	}
	
	public void testFindingPlaylistsByUri() throws Exception {

		ContentQueryBuilder playlistByUri = query().equalTo(Attributes.PLAYLIST_URI, data.mentionedOnTwitter.getCanonicalUri());
		Playlist found = Iterables.getOnlyElement(queryExecutor.executePlaylistQuery(playlistByUri.build()));
		assertThat(found.getCanonicalUri(), is(data.mentionedOnTwitter.getCanonicalUri()));
		assertThat(found.getItems(), is(Arrays.asList(data.englishForCats, data.eggsForBreakfast)));
		assertThat(found.getPlaylists(), is(Arrays.<Playlist>asList(data.eastenders, data.newsNight)));
		assertThat(Iterables.get(found.getPlaylists(), 0).getItems(), is(Arrays.<Item>asList(data.dotCottonsBigAdventure, data.peggySlapsFrank)));
		assertThat(Iterables.get(found.getPlaylists(), 1).getItems(), is(Arrays.<Item>asList(data.interviewWithMp)));

		// check that when a playlist is mentioned by uri and sub-query (e.g. item.publisher) doesn't match the playlist is still returned but with empty sublists
		ContentQueryBuilder matchesNoSubElements = query().equalTo(Attributes.PLAYLIST_URI, data.goodEastendersEpisodes.getCanonicalUri()).equalTo(Attributes.ITEM_PUBLISHER, "a made up publisher");
		Playlist emptyPlaylist = Iterables.getOnlyElement(queryExecutor.executePlaylistQuery(matchesNoSubElements.build()));
		assertThat(emptyPlaylist.getCanonicalUri(), is(data.goodEastendersEpisodes.getCanonicalUri()));
		assertThat(emptyPlaylist.getItems(), is(Collections.<Item>emptyList()));
		assertThat(emptyPlaylist.getPlaylists(), is(Collections.<Playlist>emptyList()));
	}
	
	
	public void testFilteringPlaylistsSubItems() throws Exception {
		ContentQueryBuilder query = query().equalTo(Attributes.PLAYLIST_URI, data.mentionedOnTwitter.getCanonicalUri()).equalTo(Attributes.ITEM_IS_LONG_FORM, true);
		Playlist found = Iterables.getOnlyElement(queryExecutor.executePlaylistQuery(query.build()));
		assertThat(found.getCanonicalUri(), is(data.mentionedOnTwitter.getCanonicalUri()));
		assertThat(found.getItems(), is(Arrays.asList(data.englishForCats)));
		assertThat(found.getPlaylists(), is(Arrays.<Playlist>asList(data.eastenders, data.newsNight)));
		
		
		query = query().equalTo(Attributes.PLAYLIST_URI, data.mentionedOnTwitter.getCanonicalUri()).equalTo(Attributes.ITEM_URI, data.englishForCats.getCanonicalUri()).equalTo(Attributes.LOCATION_URI, "made up uri");
		found = Iterables.getOnlyElement(queryExecutor.executePlaylistQuery(query.build()));
		assertThat(found.getCanonicalUri(), is(data.mentionedOnTwitter.getCanonicalUri()));
		Item item = Iterables.getOnlyElement(found.getItems());
		assertThat(item.getVersions(), is(Collections.<Version>emptySet()));
		assertThat(found.getPlaylists(), is(Collections.<Playlist>emptyList()));
	}
	
	/**
	 * {@link Playlist}s may contain brands that may contain items.  Item filters should be 
	 * applied to the items in the brands.  
	 */
	public void testFilteringAPlaylistsSubBrands() throws Exception {
		ContentQueryBuilder query = query().equalTo(Attributes.PLAYLIST_URI, data.mentionedOnTwitter.getCanonicalUri()).equalTo(Attributes.ITEM_URI, data.dotCottonsBigAdventure.getCanonicalUri());
		Playlist found = Iterables.getOnlyElement(queryExecutor.executePlaylistQuery(query.build()));
		assertThat(found.getCanonicalUri(), is(data.mentionedOnTwitter.getCanonicalUri()));
		assertThat(found.getItems(), is(Arrays.<Item>asList()));

		// 1) Newsnight should have been filtered since it does not contain 'dotCottonsBigAdventure'
		// 2) Eastenders be present but should only contain one item: 'dotCottonsBigAdventure'
		Brand eastenders = (Brand) Iterables.getOnlyElement(found.getPlaylists());
		assertThat(eastenders.getCanonicalUri(), is(data.eastenders.getCanonicalUri()));
		assertThat(Iterables.getOnlyElement(eastenders.getItems()).getCanonicalUri(), is(data.dotCottonsBigAdventure.getCanonicalUri()));
	}
	
	public void testFindingItemsByCurie() throws Exception {
		checkItemQuery(query().equalTo(Attributes.ITEM_URI, data.englishForCats.getCurie()), data.englishForCats);
	}
	
	public void testBrandTitleStartsWith() throws Exception {
		checkBrandQuery(query().beginning(Attributes.BRAND_TITLE, "e"), data.eastenders, data.ER);
	}
	
	public void testFindingBrandsByCurie() throws Exception {
		checkBrandQuery(query().equalTo(Attributes.BRAND_URI, data.eastenders.getCurie()), data.eastenders);
	}
	
	public void testFindingItemsInABrand() throws Exception {
		checkItemQuery(query().equalTo(Attributes.BRAND_URI, data.eastenders.getCanonicalUri()), data.peggySlapsFrank, data.dotCottonsBigAdventure);
		checkItemQuery(query().equalTo(Attributes.BRAND_URI, data.eastenders.getCurie()), data.peggySlapsFrank, data.dotCottonsBigAdventure);
	}
	
	public void testFindingBrandsInAPlaylist() throws Exception {
		checkBrandQuery(query().equalTo(Attributes.PLAYLIST_URI, data.mentionedOnTwitter.getCanonicalUri()), data.eastenders, data.newsNight);
		checkBrandQuery(query().equalTo(Attributes.PLAYLIST_URI, data.mentionedOnTwitter.getCurie()), data.eastenders, data.newsNight);
	}
	
	public void testFindingItemsInAPlaylist() throws Exception {
		checkItemQuery(query().equalTo(Attributes.PLAYLIST_URI, data.goodEastendersEpisodes.getCanonicalUri()), data.dotCottonsBigAdventure);
		checkItemQuery(query().equalTo(Attributes.PLAYLIST_URI, data.goodEastendersEpisodes.getCurie()), data.dotCottonsBigAdventure);
	}
	
	public void testFindingAvailableItems() throws Exception {
		checkItemQuery(query().equalTo(Attributes.LOCATION_AVAILABLE, true), data.brainSurgery, data.englishForCats, data.eggsForBreakfast, data.everyoneNeedsAnEel, data.interviewWithMp, data.peggySlapsFrank, data.dotCottonsBigAdventure, data.sellingStuff);
	}
	
	public void testFindingAvailableAndLongFromItems() throws Exception {
		checkItemQuery(query().equalTo(Attributes.LOCATION_AVAILABLE, true).equalTo(Attributes.ITEM_IS_LONG_FORM, true), data.englishForCats, data.dotCottonsBigAdventure, data.peggySlapsFrank, data.interviewWithMp);
	}

	public void testItemPublisherEqualsForItems() throws Exception {
		checkItemQuery(query().equalTo(ITEM_PUBLISHER, "youtube.com"), data.englishForCats, data.eggsForBreakfast);
		
		checkItemQuery(query().equalTo(ITEM_PUBLISHER, "youtube.com"), data.englishForCats, data.eggsForBreakfast); 

		checkItemQuery(query().equalTo(ITEM_PUBLISHER, "channel4.com", "youtube.com"), data.englishForCats, data.eggsForBreakfast,  data.brainSurgery);
	}
	
	
	public void testEpisodeNumberForItems() throws Exception {
		checkItemQuery(query().equalTo(EPISODE_POSITION, 2), data.peggySlapsFrank);
		
		checkItemQuery(query().lessThan(EPISODE_POSITION, 2), data.dotCottonsBigAdventure);
	}
	
	public void testTitleEqualsForItems() throws Exception {
		checkItemQuery(query().equalTo(ITEM_TITLE, "English for Cats"), data.englishForCats);
	}
	
	public void testTransportTypeEqualsForItems() throws Exception {
		checkItemQuery(query().equalTo(LOCATION_TRANSPORT_TYPE, TransportType.STREAM), data.dotCottonsBigAdventure, data.peggySlapsFrank);
	}
		
	public void testGenreEqualsForItems() throws Exception {
		checkItemQuery(query().equalTo(ITEM_GENRE, "http://uriplay.org/genres/uriplay/drama"),  data.englishForCats, data.dotCottonsBigAdventure, data.peggySlapsFrank);
		
		checkItemQuery(query().equalTo(ITEM_GENRE, "eels"),  data.everyoneNeedsAnEel);
	}
	

	public void testDurationGreaterThanForItems() throws Exception {
		checkItemQuery(query().greaterThan(VERSION_DURATION, 20),   data.dotCottonsBigAdventure, data.peggySlapsFrank, data.interviewWithMp);
		
		checkItemQuery(query().greaterThan(VERSION_DURATION, 30), data.interviewWithMp);
	}
	
	public void testTransmittedNowForItems() throws Exception {
		
		DateTime tenAm = new DateTime(2010, 10, 20, 10, 0, 0, 0, DateTimeZones.UTC);
		
		Item halfHourShowStartingAt10Am = new Item("item1", "curie:item1");
		halfHourShowStartingAt10Am.addVersion(versionWithBroadcast(tenAm, Duration.standardMinutes(30), "channel"));
		
		Item halfHourShowStartingAt11Am = new Item("item2", "curie:item2");
		halfHourShowStartingAt11Am.addVersion(versionWithBroadcast(tenAm.plusMinutes(30), Duration.standardMinutes(30), "channel"));
		
		store.createOrUpdateItem(halfHourShowStartingAt10Am);
		store.createOrUpdateItem(halfHourShowStartingAt11Am);
		
		checkItemQuery(transmissionTimeQuery(tenAm), halfHourShowStartingAt10Am);
		checkItemQuery(transmissionTimeQuery(tenAm.plusMinutes(25)), halfHourShowStartingAt10Am);
		checkItemQuery(transmissionTimeQuery(tenAm.plusMinutes(30)), halfHourShowStartingAt11Am);
		
	}
	
	public void testMultiLevelQuery() throws Exception {
		DateTime tenAm = new DateTime(2010, 10, 20, 10, 0, 0, 0, DateTimeZones.UTC);
		Item showStartingAt10Am = new Item("item1", "curie:item1");
		
		Version version1 = versionWithBroadcast(tenAm, Duration.standardMinutes(30), "c1");
		
		Version version2 = versionWithBroadcast(tenAm, Duration.standardMinutes(10), "c2");
		
		showStartingAt10Am.addVersion(version1);
		showStartingAt10Am.addVersion(version2);
		
		store.createOrUpdateItem(showStartingAt10Am);
		
		checkItemQuery(query().equalTo(Attributes.BROADCAST_ON, "c1").equalTo(Attributes.VERSION_DURATION, (int) Duration.standardMinutes(30).getStandardSeconds()), showStartingAt10Am);
		checkItemQuery(query().equalTo(Attributes.BROADCAST_ON, "c2").equalTo(Attributes.VERSION_DURATION, (int) Duration.standardMinutes(10).getStandardSeconds()), showStartingAt10Am);
		
		checkItemQueryMatchesNothing(query().equalTo(Attributes.BROADCAST_ON, "c1").equalTo(Attributes.VERSION_DURATION, (int) Duration.standardMinutes(10).getStandardSeconds()));
		
	}

	private ContentQueryBuilder transmissionTimeQuery(DateTime when) {
		return query().before(BROADCAST_TRANSMISSION_TIME, when.plusSeconds(1)).after(BROADCAST_TRANSMISSION_END_TIME, when);
	}
	
	private static Version versionWithBroadcast(DateTime start, Duration duration, String channel) {
		Version version = new Version();
		version.setDuration(duration);
		version.addBroadcast(new Broadcast(channel, start, duration));
		return version;
	}
	
	public void testTransmittedBeforeForItems() throws Exception {
		checkItemQueryMatchesNothing(query().before(BROADCAST_TRANSMISSION_TIME, DummyContentData.april22nd1930));
		
		checkItemQuery(query().before(BROADCAST_TRANSMISSION_TIME, DummyContentData.april23rd), data.dotCottonsBigAdventure);
	}
	
	public void testTransmittedAfterForItems() throws Exception {
		ContentQueryBuilder query = query().after(BROADCAST_TRANSMISSION_TIME, DummyContentData.april22nd1930); 
		checkItemQuery(query, data.englishForCats, data.eggsForBreakfast, data.everyoneNeedsAnEel, data.peggySlapsFrank, data.interviewWithMp, data.brainSurgery, data.sellingStuff);
		
		query = query().after(BROADCAST_TRANSMISSION_TIME, DummyContentData.april23rd); 
		checkItemQuery(query, data.englishForCats, data.eggsForBreakfast, data.everyoneNeedsAnEel, data.interviewWithMp, data.brainSurgery, data.sellingStuff);
	}
	
	private void checkItemQueryMatchesNothing(ContentQueryBuilder query) {
		checkItemQuery(query);
	}
	
	private void checkBrandQueryMatchesNothing(ContentQueryBuilder query) {
		checkBrandQuery(query);
	}

	private void checkItemQuery(ContentQueryBuilder query, Item... content) {
		assertThat(toUris(queryExecutor.executeItemQuery(query.build())), is(toUris(Arrays.asList(content))));
	}
	
	private void checkBrandQuery(ContentQueryBuilder query, Brand... content) {
		assertThat(toUris(queryExecutor.executeBrandQuery(query.build())), is(toUris(Arrays.asList(content))));
	}

	private Set<String> toUris(Collection<? extends Description> content) {
		Set<String> uris = Sets.newHashSet();
		for (Description description : content) {
			uris.add(description.getCanonicalUri());
		}
		return uris;
	}
}
