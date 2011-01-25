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

package org.atlasapi.persistence.content.mongo;

import static org.atlasapi.content.criteria.ContentQueryBuilder.query;
import static org.atlasapi.content.criteria.attribute.Attributes.BROADCAST_TRANSMISSION_END_TIME;
import static org.atlasapi.content.criteria.attribute.Attributes.BROADCAST_TRANSMISSION_TIME;
import static org.atlasapi.content.criteria.attribute.Attributes.LOCATION_TRANSPORT_TYPE;
import static org.atlasapi.content.criteria.attribute.Attributes.VERSION_DURATION;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import junit.framework.TestCase;

import org.atlasapi.content.criteria.ContentQuery;
import org.atlasapi.content.criteria.ContentQueryBuilder;
import org.atlasapi.content.criteria.attribute.Attributes;
import org.atlasapi.media.TransportType;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.ContentGroup;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Version;
import org.atlasapi.persistence.testing.DummyContentData;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.metabroadcast.common.persistence.MongoTestHelper;
import com.metabroadcast.common.query.Selection;
import com.metabroadcast.common.time.DateTimeZones;

public class MongoDbBackedQueryExecutorTest extends TestCase {
	
    private DummyContentData data = new DummyContentData();

    private MongoDbBackedContentStore store;
    private MongoDBQueryExecutor queryExecutor;
    
    @Override
    protected void setUp() throws Exception {
    	super.setUp();
    	
    	store = new MongoDbBackedContentStore(MongoTestHelper.anEmptyTestDatabase());
    	queryExecutor = new MongoDBQueryExecutor(store);
    	
    	store.createOrUpdate(data.eastenders, true);
    	store.createOrUpdate(data.apprentice, true);
    	store.createOrUpdate(data.newsNight, true);
    	store.createOrUpdate(data.ER, true);
    	
    	store.createOrUpdate(data.englishForCats);
    	store.createOrUpdate(data.eggsForBreakfast);
    	store.createOrUpdate(data.everyoneNeedsAnEel);
    	
    	store.createOrUpdateSkeleton(data.goodEastendersEpisodes);
    	store.createOrUpdateSkeleton(data.mentionedOnTwitter);
    }

    public void testFindingContentByUri() throws Exception {
    	assertEquals(ImmutableList.of(data.englishForCats), queryExecutor.executeUriQuery(ImmutableList.of(data.englishForCats.getCanonicalUri()), ContentQuery.MATCHES_EVERYTHING));
    	assertEquals(ImmutableList.of(data.dotCottonsBigAdventure), queryExecutor.executeUriQuery(ImmutableList.of(data.dotCottonsBigAdventure.getCanonicalUri()), ContentQuery.MATCHES_EVERYTHING));
    	assertEquals(ImmutableList.of(data.dotCottonsBigAdventure), queryExecutor.executeUriQuery(ImmutableList.of("http://dot.cotton"), ContentQuery.MATCHES_EVERYTHING));
    	assertEquals(ImmutableList.of(data.eastenders), queryExecutor.executeUriQuery(ImmutableList.of(data.eastenders.getCanonicalUri()), ContentQuery.MATCHES_EVERYTHING));
	}
    
    public void testThatIfAnItemIsFoundItIsNotFilteredOut() throws Exception {
		ContentQueryBuilder matchesNoSubElements = query().equalTo(Attributes.VERSION_DURATION, 100);
		
    	List<Identified> found = queryExecutor.executeUriQuery(ImmutableList.of(data.englishForCats.getCanonicalUri()), matchesNoSubElements.build());
    	Item item = (Item) Iterables.getOnlyElement(found);
    	
    	assertEquals(data.englishForCats, item);
		assertThat(item.getCanonicalUri(), is(data.englishForCats.getCanonicalUri()));
		assertThat(item.getVersions(), is(Collections.<Version>emptySet()));
	}

	public void testFindingMultipleBrandsByUriReturnsTheResultsInTheRightOrder() throws Exception {
    	List<Identified> brands = queryExecutor.executeUriQuery(ImmutableList.of(data.eastenders.getCanonicalUri(), data.apprentice.getCanonicalUri()), ContentQuery.MATCHES_EVERYTHING);
		assertEquals(ImmutableList.of(data.eastenders, data.apprentice), brands);
		
		brands = queryExecutor.executeUriQuery(ImmutableList.of(data.apprentice.getCanonicalUri(), data.eastenders.getCanonicalUri()), ContentQuery.MATCHES_EVERYTHING);
		assertEquals(ImmutableList.of(data.apprentice, data.eastenders), brands);
	}
	
	public void testSelections() throws Exception {
		checkDiscover(query().equalTo(Attributes.DESCRIPTION_PUBLISHER, Publisher.BBC).withSelection(new Selection(0, 3)), data.eastenders, data.apprentice, data.newsNight);
		checkDiscover(query().equalTo(Attributes.DESCRIPTION_PUBLISHER, Publisher.BBC).withSelection(new Selection(1, 3)), data.apprentice, data.newsNight, data.eelFishing);
		checkDiscover(query().equalTo(Attributes.DESCRIPTION_PUBLISHER, Publisher.BBC).withSelection(new Selection(0, 1)), data.eastenders);
		checkDiscover(query().equalTo(Attributes.DESCRIPTION_PUBLISHER, Publisher.BBC).withSelection(new Selection(1, 2)), data.apprentice, data.newsNight);
	}
	
	public void testFindingPlaylistsByUri() throws Exception {
		
		ContentGroup found = (ContentGroup) Iterables.getOnlyElement(queryExecutor.executeUriQuery(ImmutableList.of(data.mentionedOnTwitter.getCanonicalUri()), ContentQuery.MATCHES_EVERYTHING));
		assertThat(found.getCanonicalUri(), is(data.mentionedOnTwitter.getCanonicalUri()));
		
		assertThat(found.getContents(), is(ImmutableList.of(data.englishForCats, data.eggsForBreakfast, data.eastenders, data.newsNight)));
		
		assertThat(((Brand) Iterables.get(found.getContents(), 2)).getContents(), is(Arrays.<Episode>asList(data.dotCottonsBigAdventure, data.peggySlapsFrank)));
		assertThat(((Brand) Iterables.get(found.getContents(), 3)).getContents(), is(Arrays.<Episode>asList(data.interviewWithMp)));
		
		// check that when a playlist is mentioned by uri and sub-query (e.g. item.publisher) doesn't match the playlist is still returned but with empty sublists
		ContentGroup emptyPlaylist = (ContentGroup) Iterables.getOnlyElement(queryExecutor.executeUriQuery(ImmutableList.of(data.goodEastendersEpisodes.getCanonicalUri()), query().equalTo(Attributes.DESCRIPTION_PUBLISHER, Publisher.TVBLOB).build()));
		assertThat(emptyPlaylist.getCanonicalUri(), is(data.goodEastendersEpisodes.getCanonicalUri()));
		assertThat(emptyPlaylist.getContents(), is(Collections.<Content>emptyList()));
	}
	
	public void testFindingItemsByCurie() throws Exception {
    	assertEquals(ImmutableList.of(data.englishForCats), queryExecutor.executeUriQuery(ImmutableList.of(data.englishForCats.getCurie()), ContentQuery.MATCHES_EVERYTHING));
	}
	
	public void testFindingBrandsByCurie() throws Exception {
    	assertEquals(ImmutableList.of(data.eastenders), queryExecutor.executeUriQuery(ImmutableList.of(data.eastenders.getCurie()), ContentQuery.MATCHES_EVERYTHING));
	}
	
	public void testFindingAvailableItems() throws Exception {
		checkDiscover(query().equalTo(Attributes.LOCATION_AVAILABLE, true), data.apprentice, data.englishForCats, data.eggsForBreakfast, data.eelFishing, data.newsNight, data.eastenders);
	}
	
	public void testFindingAvailableAndLongFormContent() throws Exception {
		checkDiscover(query().equalTo(Attributes.LOCATION_AVAILABLE, true).equalTo(Attributes.ITEM_IS_LONG_FORM, true), data.englishForCats, data.eastenders, data.newsNight);
	}

	public void testItemPublisherEquality() throws Exception {
		checkDiscover(query().equalTo(Attributes.DESCRIPTION_PUBLISHER, Publisher.YOUTUBE), data.englishForCats, data.eggsForBreakfast);
		
		checkDiscover(query().equalTo(Attributes.DESCRIPTION_PUBLISHER, Publisher.YOUTUBE), data.englishForCats, data.eggsForBreakfast); 

		checkDiscover(query().isAnEnumIn(Attributes.DESCRIPTION_PUBLISHER, ImmutableList.<Enum<Publisher>>of(Publisher.C4, Publisher.YOUTUBE)), data.englishForCats, data.eggsForBreakfast);
	}
	
	public void testTransportTypeEqualsForItems() throws Exception {
		checkDiscover(query().equalTo(LOCATION_TRANSPORT_TYPE, TransportType.STREAM), data.eastenders);
	}
		
	public void testGenreEqualsForItems() throws Exception {
		checkDiscover(query().equalTo(Attributes.DESCRIPTION_GENRE, "http://ref.atlasapi.org/genres/atlas/drama"),  data.englishForCats, data.eastenders);
		checkDiscover(query().equalTo(Attributes.DESCRIPTION_GENRE, "eels"),  data.eelFishing);
	}

	public void testDurationGreaterThanForItems() throws Exception {
		checkDiscover(query().greaterThan(VERSION_DURATION, 20),   data.eastenders, data.newsNight);
		checkDiscover(query().greaterThan(VERSION_DURATION, 30), data.newsNight);
	}
	
	public void testTransmittedNowForItems() throws Exception {
		
		DateTime tenAm = new DateTime(2010, 10, 20, 10, 0, 0, 0, DateTimeZones.UTC);
		
		Item halfHourShowStartingAt10Am = new Item("item1", "curie:item1", Publisher.BBC);
		halfHourShowStartingAt10Am.addVersion(versionWithBroadcast(tenAm, Duration.standardMinutes(30), "channel"));
		
		Item halfHourShowStartingAt11Am = new Item("item2", "curie:item2", Publisher.BBC);
		halfHourShowStartingAt11Am.addVersion(versionWithBroadcast(tenAm.plusMinutes(30), Duration.standardMinutes(30), "channel"));
		
		store.createOrUpdate(halfHourShowStartingAt10Am);
		store.createOrUpdate(halfHourShowStartingAt11Am);
		
		checkDiscover(transmissionTimeQuery(tenAm), halfHourShowStartingAt10Am);
		checkDiscover(transmissionTimeQuery(tenAm.plusMinutes(25)), halfHourShowStartingAt10Am);
		checkDiscover(transmissionTimeQuery(tenAm.plusMinutes(30)), halfHourShowStartingAt11Am);
	}
	
	public void testMultiLevelQuery() throws Exception {
		DateTime tenAm = new DateTime(2010, 10, 20, 10, 0, 0, 0, DateTimeZones.UTC);
		Item showStartingAt10Am = new Item("item1", "curie:item1", Publisher.BBC);
		
		Version version1 = versionWithBroadcast(tenAm, Duration.standardMinutes(30), "c1");
		
		Version version2 = versionWithBroadcast(tenAm, Duration.standardMinutes(10), "c2");
		
		showStartingAt10Am.addVersion(version1);
		showStartingAt10Am.addVersion(version2);
		
		store.createOrUpdate(showStartingAt10Am);
		
		checkDiscover(query().equalTo(Attributes.BROADCAST_ON, "c1").equalTo(Attributes.VERSION_DURATION, (int) Duration.standardMinutes(30).getStandardSeconds()), showStartingAt10Am);
		checkDiscover(query().equalTo(Attributes.BROADCAST_ON, "c2").equalTo(Attributes.VERSION_DURATION, (int) Duration.standardMinutes(10).getStandardSeconds()), showStartingAt10Am);
		
		checkDiscoverMatchesNothing(query().equalTo(Attributes.BROADCAST_ON, "c1").equalTo(Attributes.VERSION_DURATION, (int) Duration.standardMinutes(10).getStandardSeconds()));
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
		checkDiscoverMatchesNothing(query().before(BROADCAST_TRANSMISSION_TIME, DummyContentData.april22nd1930));
		
		List<Content> found = queryExecutor.discover(query().before(BROADCAST_TRANSMISSION_TIME, DummyContentData.april23rd).build());
		
		Container<?> container = (Container<?>) Iterables.getOnlyElement(found);
		
		assertEquals(data.eastenders, container);
		assertEquals(ImmutableList.of(data.dotCottonsBigAdventure), container.getContents());
	}
	
	public void testTransmittedAfterForItems() throws Exception {
		ContentQueryBuilder query = query().after(BROADCAST_TRANSMISSION_TIME, DummyContentData.april22nd1930); 
		checkDiscover(query, data.englishForCats, data.eggsForBreakfast, data.eelFishing, data.eastenders, data.newsNight, data.apprentice);
		
		query = query().after(BROADCAST_TRANSMISSION_TIME, DummyContentData.april23rd); 
		checkDiscover(query, data.englishForCats, data.eggsForBreakfast, data.eelFishing, data.newsNight, data.apprentice);
	}
	
	private void checkDiscoverMatchesNothing(ContentQueryBuilder query) {
		checkDiscover(query);
	}

	private void checkDiscover(ContentQueryBuilder query, Content... content) {
		assertThat(toUris(queryExecutor.discover(query.build())), is(toUris(Arrays.asList(content))));
	}

	private Set<String> toUris(Iterable<? extends Identified> content) {
		Set<String> uris = Sets.newHashSet();
		for (Identified description : content) {
			uris.add(description.getCanonicalUri());
		}
		return uris;
	}
}
