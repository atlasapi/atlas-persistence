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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import junit.framework.TestCase;

import org.atlasapi.content.criteria.ContentQuery;
import org.atlasapi.media.TransportType;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.ContentGroup;
import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.Encoding;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Location;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.LookupRef.LookupType;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Series;
import org.atlasapi.media.entity.Version;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.lookup.NewLookupWriter;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.atlasapi.persistence.testing.DummyContentData;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.metabroadcast.common.persistence.MongoTestHelper;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.time.TimeMachine;

public class MongoDbBackedContentStoreTest extends TestCase {
	
	private final TimeMachine clock = new TimeMachine();
	private final DatabasedMongo mongo = MongoTestHelper.anEmptyTestDatabase();
	
	private final NewLookupWriter lookupWriter = new NewLookupWriter() {
        @Override
        public void ensureLookup(Described described) {
        }
    };
    
	private final ContentWriter writer = new MongoContentWriter(mongo, lookupWriter, clock);;
	private final ContentResolver resolver = new MongoContentResolver(mongo);
	private final DummyContentData data = new DummyContentData();
    
    public void testThatWhenFindingByUriContentThatIsACanonicalUriMatchIsUsed() throws Exception {
        
    	Item a = new Item("a", "curie:a", Publisher.BBC);
    	a.addAlias("b");
    	Item b = new Item("b", "curie:b", Publisher.C4);
    	b.addAlias("a");
    	
    	writer.createOrUpdate(a);
    	writer.createOrUpdate(b);
    	
    	LookupRef aLookupRef = new LookupRef("a", Publisher.BBC, LookupType.TOP_LEVEL_ITEM);
        assertEquals("a", resolver.findByCanonicalUris(ImmutableList.of(aLookupRef)).get(aLookupRef).requireValue().getCanonicalUri());
        
        LookupRef bLookupRef = new LookupRef("b", Publisher.C4, LookupType.TOP_LEVEL_ITEM);
        assertEquals("b", resolver.findByCanonicalUris(ImmutableList.of(bLookupRef)).get(bLookupRef).requireValue().getCanonicalUri());
	}

//    public void testShouldCreateAndRetrieveItem() throws Exception {
//        writer.createOrUpdate(data.eggsForBreakfast);
//        writer.createOrUpdate(data.englishForCats);
//
//        List<Identified> items = resolver.findByLookupRefs(ImmutableList.of(data.eggsForBreakfast.getCanonicalUri()));
//        assertNotNull(items);
//        assertEquals(1, items.size());
//        Item item = (Item) items.get(0);
//		assertEquals(data.eggsForBreakfast.getTitle(), item.getTitle());
//        assertNotNull(item.getLastFetched());
//
//        items = resolver.findByLookupRefs(ImmutableList.of(data.eggsForBreakfast.getCanonicalUri(), data.englishForCats.getCanonicalUri()));
//        assertEquals(2, items.size());
//
//        writer.createOrUpdate(data.eggsForBreakfast);
//
//        items = resolver.findByLookupRefs(ImmutableList.of(data.eggsForBreakfast.getCanonicalUri()));
//        assertNotNull(items);
//        assertEquals(1, items.size());
//        assertEquals(data.eggsForBreakfast.getTitle(), ((Item) items.get(0)).getTitle());
//        assertEquals(data.eggsForBreakfast.getCurie(), items.get(0).getCurie());
//        
//        
//        Set<String> aliases = items.get(0).getAliases();
//        for (String alias: aliases) {
//            assertNotSame(items.get(0).getCanonicalUri(), alias);
//        }
//    }
//    
//    public void testEpisodesAreAddedToBrands() throws Exception {
//       
//    	writer.createOrUpdate(data.eastenders);
//
//        Episode nextWeeksEastenders = new Episode("next-week", "bbc:next-week", Publisher.BBC);
//        nextWeeksEastenders.setContainer(new Brand(data.eastenders.getCanonicalUri(), "wrong curie", Publisher.BBC));
//       
//        writer.createOrUpdate(nextWeeksEastenders);
//        
//        Brand foundBrand = (Brand) resolver.findByLookupRefs(data.eastenders.getCanonicalUri());
//
//        assertEquals(data.eastenders.getCurie(), foundBrand.getCurie());
//
//        assertEquals(data.eastenders.getContents().size() + 1, foundBrand.getContents().size());
//        assertTrue(foundBrand.getContents().contains(nextWeeksEastenders)); 
//	}
//
//    public void testShouldCreateAndRetrievePlaylist() throws Exception {
//        writer.createOrUpdate(data.eastenders);
//    	writer.createOrUpdateSkeleton(data.goodEastendersEpisodes);
//
//        List<String> itemUris = Lists.newArrayList();
//        for (Content item : data.goodEastendersEpisodes.getContents()) {
//            itemUris.add(item.getCanonicalUri());
//        }
//        List<Identified> items = resolver.findByLookupRefs(itemUris);
//        assertNotNull(items);
//        assertEquals(1, items.size());
//        assertEquals(data.dotCottonsBigAdventure.getTitle(), ((Item) items.get(0)).getTitle());
//
//        writer.createOrUpdateSkeleton(data.goodEastendersEpisodes);
//
//        List<Identified> playlists = resolver.findByLookupRefs(ImmutableList.of(data.goodEastendersEpisodes.getCanonicalUri()));
//        assertNotNull(playlists);
//        assertEquals(1, playlists.size());
//        
//        ContentGroup groupFound = (ContentGroup) playlists.get(0);
//        
//		assertEquals(data.goodEastendersEpisodes.getTitle(), groupFound.getTitle());
//        assertEquals(data.goodEastendersEpisodes.getCurie(), groupFound.getCurie());
//        assertNotNull(groupFound.getLastFetched());
//        
//        Set<String> aliases = groupFound.getAliases();
//        for (String alias: aliases) {
//            assertNotSame(groupFound.getCanonicalUri(), alias);
//        }
//
//        Collection<String> uris = groupFound.getContentUris();
//        assertTrue(uris.size() > 0);
//        assertEquals(data.goodEastendersEpisodes.getContentUris().size(), uris.size());
//
//        List<Content> playlistItems = groupFound.getContents();
//        assertTrue(playlistItems.size() > 0);
//        Item firstItem = (Item) data.goodEastendersEpisodes.getContents().iterator().next();
//        assertEquals(firstItem.getTitle(), playlistItems.iterator().next().getTitle());
//    }
//    
////    public void testShouldNotAllowCreationOfPlaylistWhenContentNotExist() throws Exception {
////        try {
////            writer.createOrUpdateSkeleton(data.goodEastendersEpisodes);
////            fail("Should have thrown exception");
////        } catch (GroupContentNotExistException e) {
////            assertTrue(e.getMessage().contains(data.goodEastendersEpisodes.getCanonicalUri()));
////        }
////    }
//
//	public void testShouldMarkUnavailableItemsAsUnavailable() throws Exception {
//
//		Episode eggs = new Episode("eggs", "eggs", Publisher.C4);
//		eggs.addVersion(DummyContentData.versionWithEmbeddableLocation(new DateTime(), Duration.standardDays(1), TransportType.LINK));
//		
//		Episode eel = new Episode("eel", "eel", Publisher.C4);
//		eel.addVersion(DummyContentData.versionWithEmbeddableLocation(new DateTime(), Duration.standardDays(1), TransportType.LINK));
//		
//		Episode english = new Episode("english", "english", Publisher.C4);
//		english.addVersion(DummyContentData.versionWithEmbeddableLocation(new DateTime(), Duration.standardDays(1), TransportType.LINK));
//		
//		data.eastenders.addContents(eggs);
//
//		writer.createOrUpdate(data.eastenders);
//
//		List<Identified> playlists = resolver.findByLookupRefs(Lists.newArrayList(data.eastenders.getCanonicalUri()));
//		
//		assertEquals(1, playlists.size());
//		assertEquals(3, ((Container<?>) playlists.get(0)).getContents().size());
//
//		data.eastenders.setContents(ImmutableList.of(eggs, english));
//
//		writer.createOrUpdate(data.eastenders, true);
//
//		playlists = resolver.findByLookupRefs(Lists.newArrayList(data.eastenders.getCanonicalUri()));
//		assertEquals(1, playlists.size());
//		List<? extends Item> items = ((Container<?>) playlists.get(0)).getContents();
//		assertEquals(4, items.size());
//
//		for (Item item : items) {
//			if (item.equals(eggs) || item.equals(english)) {
//				assertTrue(isAvailable(item));
//			} else {
//				assertFalse(isAvailable(item));
//			}
//		}
//
//		data.eastenders.setContents(ImmutableList.of(eel, english));
//
//		writer.createOrUpdate(data.eastenders, true);
//
//		playlists = resolver.findByLookupRefs(ImmutableList.of(data.eastenders.getCanonicalUri()));
//		assertEquals(1, playlists.size());
//		
//		items = ((Container<?>) playlists.get(0)).getContents();
//		assertEquals(5, items.size());
//
//		for (Item item : items) {
//			if (item.equals(eel) || item.equals(english)) {
//				assertTrue(isAvailable(item));
//			} else {
//				assertFalse(isAvailable(item));
//			}
//		}
//	}
//
//    private boolean isAvailable(Item item) {
//        assertFalse(item.getVersions().isEmpty());
//        for (Version version : item.getVersions()) {
//            assertFalse(version.getManifestedAs().isEmpty());
//            for (Encoding encoding : version.getManifestedAs()) {
//                assertFalse(encoding.getAvailableAt().isEmpty());
//                for (Location location : encoding.getAvailableAt()) {
//                    return location.getAvailable();
//                }
//            }
//        }
//        return false;
//    }
//
//    public void testShouldIncludeEpisodeBrandSummary() throws Exception {
//        writer.createOrUpdate(data.theCreditCrunch);
//
//        List<Identified> items = resolver.findByLookupRefs(ImmutableList.of(data.theCreditCrunch.getCanonicalUri()));
//        assertNotNull(items);
//        assertEquals(1, items.size());
//        assertTrue(items.get(0) instanceof Episode);
//        
//        Episode episode = (Episode) items.get(0);
//        assertEquals(data.theCreditCrunch.getTitle(), episode.getTitle());
//        
//        Container<?> brandSummary = episode.getContainer();
//        assertNotNull(brandSummary);
//        assertEquals(data.dispatches.getCanonicalUri(), brandSummary.getCanonicalUri());
//    }
//    
//    public void testShouldGetBrandOrPlaylist() throws Exception {
//    	writer.createOrUpdate(data.eastenders,);
//        writer.createOrUpdateSkeleton(data.goodEastendersEpisodes);
//        
//        List<Identified> playlists = resolver.findByLookupRefs(ImmutableList.of(data.goodEastendersEpisodes.getCanonicalUri()));
//        assertEquals(1, playlists.size());
//        assertFalse(playlists.get(0) instanceof Brand);
//        
//        writer.createOrUpdate(data.dispatches);
//        playlists = resolver.findByLookupRefs(Lists.newArrayList(data.dispatches.getCanonicalUri()));
//        assertEquals(1, playlists.size());
//        assertTrue(playlists.get(0) instanceof Brand);
//    }
//    
//    public void testShouldGetEpisodeOrItem() throws Exception {
//        writer.createOrUpdate(data.englishForCats);
//        
//        List<Identified> items = resolver.findByLookupRefs(ImmutableList.of(data.englishForCats.getCanonicalUri()));
//        assertEquals(1, items.size());
//        assertFalse(items.get(0) instanceof Episode);
//        
//        writer.createOrUpdate(data.brainSurgery);
//        items = resolver.findByLookupRefs(ImmutableList.of(data.brainSurgery.getCanonicalUri()));
//        assertEquals(1, items.size());
//        assertTrue(items.get(0) instanceof Episode);
//    }
//    
//    public void testShouldGetEpisodeThroughAnonymousMethods() throws Exception {
//        writer.createOrUpdate(data.brainSurgery);
//        
//        Identified episode = resolver.findByLookupRefs(data.brainSurgery.getCanonicalUri());
//        assertNotNull(episode);
//        assertTrue(episode instanceof Episode);
//    }
//    
//    public void ignoreShouldPreserveAliases() throws Exception {
//        data.theCreditCrunch.setAliases(Sets.newHashSet("somealias"));
//        writer.createOrUpdate(data.theCreditCrunch);
//        
//        data.theCreditCrunch.setAliases(Sets.newHashSet("anotheralias", "blah"));
//        writer.createOrUpdate(data.theCreditCrunch);
//        
//        List<Identified> items = resolver.findByLookupRefs(ImmutableList.of(data.theCreditCrunch.getCanonicalUri()));
//        assertNotNull(items);
//        assertEquals(1, items.size());
//        assertEquals(3, items.get(0).getAliases().size());
//    }
//    
//    public void testShouldProcessSubPlaylists() throws Exception {
//    	writer.createOrUpdate(data.eastenders);
//    	writer.createOrUpdate(data.neighbours);
//
//    	writer.createOrUpdateSkeleton(data.goodEastendersEpisodes);
//        
//        data.goodEastendersEpisodes.addContents(ImmutableList.of(data.neighbours));
//        writer.createOrUpdateSkeleton(data.goodEastendersEpisodes);
//        
//        List<Identified> playlists = resolver.findByLookupRefs(ImmutableList.of(data.goodEastendersEpisodes.getCanonicalUri()));
//        assertEquals(1, playlists.size());
//        
//        ContentGroup group = (ContentGroup) playlists.get(0);
//        assertEquals(ImmutableList.of(data.dotCottonsBigAdventure, data.neighbours), group.getContents());
//    }
//    
//    public void testShouldListAllItems() throws Exception {
//        Item item1 = new Item("1", "1", Publisher.BLIP);
//        Item item2 = new Item("2", "2", Publisher.BLIP);
//        Item item3 = new Item("3", "3", Publisher.BLIP);
//		writer.createOrUpdate(item1);
//		writer.createOrUpdate(item2);
//		writer.createOrUpdate(item3);
//        
//        ImmutableList<Content> items = ImmutableList.copyOf(store.listAllRoots(null, 2));
//        
//        assertEquals(ImmutableList.of(item1, item2), items);
//        
//        items = ImmutableList.copyOf(store.listAllRoots(item2.getCanonicalUri(), 2));
//        
//        assertEquals(ImmutableList.of(item3), items);
//    }
//    
//    public void testShouldListAllPlaylists() throws Exception {
//        writer.createOrUpdate(data.dispatches);
//        writer.createOrUpdate(data.eastenders);
//        
//        List<Content> items = ImmutableList.copyOf(store.listAllRoots(null, 2));
//        assertEquals(ImmutableList.of(data.eastenders, data.dispatches), items);
//    }
//    
//    public void testThatItemsAreNotRemovedFromTheirBrands() throws Exception {
//    	String itemUri = "itemUri";
//    	String brandUri = "brandUri";
//
//		Brand brand = new Brand(brandUri, "brand:curie", Publisher.BBC);
//		brand.setContents(new Episode(itemUri, "item:curie", Publisher.BBC));
//		
//    	writer.createOrUpdate(brand);
//    	
//    	assertThat((Brand) ((Episode) resolver.findByLookupRefs(itemUri)).getContainer(), is(brand)); 
//    	
//    	writer.createOrUpdate((new Episode(itemUri, "item:curie", Publisher.BBC)));
//
//    	assertThat((Brand) ((Item) resolver.findByLookupRefs(itemUri)).getContainer(), is(brand)); 
//	}
//    
////    public void testThatSavingASkeletalPlaylistThatContainsSubElementsThatArentInTheDBThrowsAnException() throws Exception {
////    	Item item = new Item("1", "1", Publisher.BLIP);
////    	Item item2 = new Item("2", "2", Publisher.BLIP);
////
////    	ContentGroup playlist = new ContentGroup();
////		playlist.setCanonicalUri("playlist");
////		playlist.setContents(item, item2, item2);
////		
////		try { 
////			writer.createOrUpdateSkeleton(playlist);
////			fail();
////		} catch (GroupContentNotExistException e) {
////			// expected
////		}
////		
////		writer.createOrUpdate(item);
////		writer.createOrUpdate(item2);
////		
////		// should be ok now because the items are in the db
////		writer.createOrUpdateSkeleton(playlist);
////		
////		Container<Item> subplaylist = new Container<Item>("3", "3", Publisher.YOUTUBE);
////
////		playlist.setContents(item, item2, item2, subplaylist);
////		
////		try { 
////			writer.createOrUpdateSkeleton(playlist);
////			fail();
////		} catch (GroupContentNotExistException e) {
////			// expected
////		}
////	}
//    
//    public void testPersistingASkeletalPlaylist() throws Exception {
//    	
//    	Item item1 = new Item("i1", "1", Publisher.BLIP);
//    	Item item2 = new Item("i2", "2", Publisher.BLIP);
//    	Container<Item> subplaylist = new Container<Item>("subplaylist", "subplaylist", Publisher.BBC);
//
//    	writer.createOrUpdate(item1);
//		writer.createOrUpdate(item2);
//		writer.createOrUpdate(subplaylist);
//    	
//    	ContentGroup playlist = new ContentGroup();
//		playlist.setCanonicalUri("playlist");
//		playlist.setContents(item1, item2, subplaylist);
//		
//		writer.createOrUpdateSkeleton(playlist);
//		
//		ContentGroup found = (ContentGroup) resolver.findByLookupRefs("playlist");
//		
//		assertEquals(clock.now(), found.getLastFetched());
//		assertEquals(clock.now(), found.getFirstSeen());
//		
//		assertEquals(3, found.getContents().size());
//	}
//    
//    public void testPersistingTopLevelSeries() throws Exception {
//    	Episode ep2 = new Episode("2", "2", Publisher.BBC);
//
//    	Series series = new Series("1", "1", Publisher.BBC).withSeriesNumber(10);
//		series.addContents(ep2);
//		
//		writer.createOrUpdate(series);
//    	
//		Series found = (Series) resolver.findByLookupRefs(series.getCanonicalUri());
//		
//		assertEquals(10, (int) found.getSeriesNumber());
//		
//		assertEquals(ImmutableList.of(ep2), found.getContents());
//		
//		Episode foundEpisode = (Episode) resolver.findByLookupRefs(ep2.getCanonicalUri());
//		assertEquals(series, foundEpisode.getContainer());
//	}
//    
//    public void testPersistingSeriesThatArePartOfABrand() throws Exception {
//    	Episode ep2 = new Episode("2", "2", Publisher.BBC);
//
//    	Brand brand = new Brand("brand", "brand", Publisher.BBC);
//    	brand.setContents(ep2);
//    	
//    	Series series = new Series("series", "series", Publisher.BBC).withSeriesNumber(10);
//    	series.setContents(ep2);
//    	
//		writer.createOrUpdate(brand);
//		
//		assertEquals(ImmutableList.of(brand), store.discover(ContentQuery.MATCHES_EVERYTHING));
//		
//		Episode foundEpisode = (Episode) resolver.findByLookupRefs(ep2.getCanonicalUri());
//		
//		assertEquals(series, foundEpisode.getSeries());
//		assertEquals(brand, foundEpisode.getContainer());
//		
//		Series found = (Series) resolver.findByLookupRefs(series.getCanonicalUri());
//		assertEquals(10, (int) found.getSeriesNumber());
//	}
//    
//    public void testRetrieveSeriesEmbededInBrand() throws Exception {
//        Episode ep2 = new Episode("2", "2", Publisher.BBC);
//
//        Brand brand = new Brand("brand", "brand", Publisher.BBC);
//        brand.setContents(ep2);
//        
//        Series series = new Series("series", "series", Publisher.BBC).withSeriesNumber(10);
//        series.setContents(ep2);
//        
//        writer.createOrUpdate(brand);
//        
//        Identified content = resolver.findByLookupRefs("series");
//        assertEquals(series, (Series) content);
//    }
}
