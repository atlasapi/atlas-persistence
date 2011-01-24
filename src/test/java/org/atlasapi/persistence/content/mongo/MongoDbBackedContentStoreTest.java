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
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import junit.framework.TestCase;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Description;
import org.atlasapi.media.entity.Encoding;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Location;
import org.atlasapi.media.entity.Playlist;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Version;
import org.atlasapi.persistence.testing.DummyContentData;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.metabroadcast.common.persistence.MongoTestHelper;
import com.metabroadcast.common.time.TimeMachine;

public class MongoDbBackedContentStoreTest extends TestCase {
	
	private final TimeMachine clock = new TimeMachine();
	
	private MongoDbBackedContentStore store;
	private DummyContentData data ;
    
    @Override
    protected void setUp() throws Exception {
    	super.setUp();
    	this.store = new MongoDbBackedContentStore(MongoTestHelper.anEmptyTestDatabase(), clock);
    	data = new DummyContentData();
    }
    
    public void testSavesAliasesForItems() throws Exception {
    	
    	data.englishForCats.addAlias("c");
    	
        store.createOrUpdateItem(data.englishForCats);
        
        store.addAliases(data.englishForCats.getCanonicalUri(), ImmutableSet.of("a", "b"));
        
        assertEquals(ImmutableSet.of("a", "b", "c"), store.findByUri(data.englishForCats.getCanonicalUri()).getAliases()); 
	}
    
    public void testThatWhenFindingByUriContentThatIsACanonicalUriMatchIsUsed() throws Exception {
        
    	Item a = new Item("a", "curie:a", Publisher.BBC);
    	a.addAlias("b");
    	Item b = new Item("b", "curie:b", Publisher.C4);
    	b.addAlias("a");
    	
    	store.createOrUpdateItem(a);
    	store.createOrUpdateItem(b);
    	
    	assertEquals("a", store.findByUri("a").getCanonicalUri());
    	assertEquals("b", store.findByUri("b").getCanonicalUri());
	}
    
    public void testSavesAliasesForPlaylists() throws Exception {
        store.createOrUpdatePlaylist(data.eastenders, true);
        store.addAliases(data.eastenders.getCanonicalUri(), ImmutableSet.of("a", "b"));
        assertTrue(store.findByUri(data.eastenders.getCanonicalUri()).getAliases().containsAll(ImmutableSet.of("a", "b"))); 
    }

    public void testShouldCreateAndRetrieveItem() throws Exception {
        data.eggsForBreakfast.setContainedIn(Sets.<Playlist> newHashSet(data.eastenders));
        store.createOrUpdateItem(data.eggsForBreakfast);
        store.createOrUpdateItem(data.englishForCats);

        List<Item> items = store.findItemsByCanonicalUri(Lists.newArrayList(data.eggsForBreakfast.getCanonicalUri()));
        assertNotNull(items);
        assertEquals(1, items.size());
        assertEquals(data.eggsForBreakfast.getTitle(), items.get(0).getTitle());
        assertNotNull(items.get(0).getLastFetched());

        items = store.findItemsByCanonicalUri(Lists.newArrayList(data.eggsForBreakfast.getCanonicalUri(), data.englishForCats.getCanonicalUri()));
        assertEquals(2, items.size());

        store.createOrUpdateItem(data.eggsForBreakfast);

        items = store.findItemsByCanonicalUri(Lists.newArrayList(data.eggsForBreakfast.getCanonicalUri()));
        assertNotNull(items);
        assertEquals(1, items.size());
        assertEquals(data.eggsForBreakfast.getTitle(), items.get(0).getTitle());
        assertEquals(data.eggsForBreakfast.getCurie(), items.get(0).getCurie());
        
        Set<String> containedInUris = items.get(0).getContainedInUris();
        assertEquals(2, containedInUris.size());
        assertEquals(data.eastenders.getCanonicalUri(), Iterables.get(containedInUris, 1));
        assertEquals(data.mentionedOnTwitter.getCanonicalUri(), Iterables.get(containedInUris, 0));
        
        
        Set<String> aliases = items.get(0).getAliases();
        for (String alias: aliases) {
            assertNotSame(items.get(0).getCanonicalUri(), alias);
        }
    }
    
    public void testEpisodesAreAddedToBrands() throws Exception {
       
    	store.createOrUpdatePlaylist(data.eastenders, false);

        Episode nextWeeksEastenders = new Episode("next-week", "bbc:next-week", Publisher.BBC);
        nextWeeksEastenders.setBrand(new Brand(data.eastenders.getCanonicalUri(), "wrong curie", Publisher.BBC));
       
        store.createOrUpdateItem(nextWeeksEastenders);
        
        Brand brand = (Brand) store.findByUri(data.eastenders.getCanonicalUri());

        assertEquals(data.eastenders.getCurie(), brand.getCurie());

        assertEquals(data.eastenders.getItems().size() + 1, brand.getItems().size());
        assertTrue(brand.getItems().contains(nextWeeksEastenders)); 
	}

    public void testShouldCreateAndRetrievePlaylist() throws Exception {
        store.createOrUpdatePlaylist(data.goodEastendersEpisodes, false);

        List<String> itemUris = Lists.newArrayList();
        for (Item item : data.goodEastendersEpisodes.getItems()) {
            itemUris.add(item.getCanonicalUri());
        }
        List<Item> items = store.findItemsByCanonicalUri(itemUris);
        assertNotNull(items);
        assertEquals(1, items.size());
        assertEquals(data.dotCottonsBigAdventure.getTitle(), items.get(0).getTitle());

        store.createOrUpdatePlaylist(data.goodEastendersEpisodes, false);

        List<Playlist> playlists = store.findHydratedPlaylistsByCanonicalUri(Lists.newArrayList(data.goodEastendersEpisodes.getCanonicalUri()));
        assertNotNull(playlists);
        assertEquals(1, playlists.size());
        assertEquals(data.goodEastendersEpisodes.getTitle(), playlists.get(0).getTitle());
        assertEquals(data.goodEastendersEpisodes.getCurie(), playlists.get(0).getCurie());
        assertNotNull(playlists.get(0).getLastFetched());
        assertEquals(DummyContentData.april23rd, playlists.get(0).getThisOrChildLastUpdated());
        
        Set<String> aliases = playlists.get(0).getAliases();
        for (String alias: aliases) {
            assertNotSame(playlists.get(0).getCanonicalUri(), alias);
        }

        Collection<String> uris = playlists.get(0).getItemUris();
        assertTrue(uris.size() > 0);
        assertEquals(data.goodEastendersEpisodes.getItemUris().size(), uris.size());

        List<Item> playlistItems = playlists.get(0).getItems();
        assertTrue(playlistItems.size() > 0);
        Item firstItem = data.goodEastendersEpisodes.getItems().iterator().next();
        assertEquals(firstItem.getTitle(), playlistItems.iterator().next().getTitle());
    }
    
    public void testShouldCreateElementsInPlaylistIfRequested() throws Exception {

    	Playlist beginningWithA = new Playlist("/a", "curie", Publisher.BBC);
    	beginningWithA.setPlaylists(Lists.<Playlist>newArrayList(data.eastenders));
    	
        store.createOrUpdatePlaylist(beginningWithA, true);
    	
        assertTrue(store.findHydratedPlaylistsByCanonicalUri(Lists.newArrayList(data.eastenders.getCanonicalUri())).get(0).getContainedInUris().contains("/a"));
	}

    public void testShouldMarkUnavailableItemsAsUnavailable() throws Exception {
        data.goodEastendersEpisodes.addItem(data.eggsForBreakfast);
        store.createOrUpdatePlaylist(data.goodEastendersEpisodes, false);

        List<Playlist> playlists = store.findHydratedPlaylistsByCanonicalUri(Lists.newArrayList(data.goodEastendersEpisodes.getCanonicalUri()));
        assertEquals(1, playlists.size());
        assertEquals(2, playlists.get(0).getItems().size());

        List<Item> newItems = Lists.newArrayList();
        newItems.add(data.eggsForBreakfast);
        newItems.add(data.englishForCats);
        data.goodEastendersEpisodes.setItems(newItems);
        assertEquals(2, data.goodEastendersEpisodes.getItems().size());
        assertEquals(2, data.goodEastendersEpisodes.getItemUris().size());

        store.createOrUpdatePlaylist(data.goodEastendersEpisodes, true);

        playlists = store.findHydratedPlaylistsByCanonicalUri(Lists.newArrayList(data.goodEastendersEpisodes.getCanonicalUri()));
        assertEquals(1, playlists.size());
        List<Item> items = playlists.get(0).getItems();
        assertEquals(3, items.size());

        for (Item item : items) {
            if (item.getCanonicalUri().equals(data.dotCottonsBigAdventure.getCanonicalUri())) {
                assertFalse(isAvailable(item));
            } else {
                assertTrue(isAvailable(item));
            }
        }

        newItems = Lists.newArrayList();
        newItems.add(data.everyoneNeedsAnEel);
        newItems.add(data.englishForCats);
        data.goodEastendersEpisodes.setItems(newItems);
        assertEquals(2, data.goodEastendersEpisodes.getItems().size());
        assertEquals(2, data.goodEastendersEpisodes.getItemUris().size());

        store.createOrUpdatePlaylist(data.goodEastendersEpisodes, true);

        playlists = store.findHydratedPlaylistsByCanonicalUri(Lists.newArrayList(data.goodEastendersEpisodes.getCanonicalUri()));
        assertEquals(1, playlists.size());
        items = playlists.get(0).getItems();
        assertEquals(4, items.size());

        for (Item item : items) {
            if (item.getCanonicalUri().equals(data.dotCottonsBigAdventure.getCanonicalUri()) || item.getCanonicalUri().equals(data.eggsForBreakfast.getCanonicalUri())) {
                assertFalse(isAvailable(item));
            } else {
                assertTrue(isAvailable(item));
            }
        }
    }

    private boolean isAvailable(Item item) {
        assertFalse(item.getVersions().isEmpty());
        for (Version version : item.getVersions()) {
            assertFalse(version.getManifestedAs().isEmpty());
            for (Encoding encoding : version.getManifestedAs()) {
                assertFalse(encoding.getAvailableAt().isEmpty());
                for (Location location : encoding.getAvailableAt()) {
                    return location.getAvailable();
                }
            }
        }
        return false;
    }

    public void testShouldIncludeEpisodeBrandSummary() throws Exception {
        store.createOrUpdateItem(data.theCreditCrunch);

        List<Item> items = store.findItemsByCanonicalUri(Lists.newArrayList(data.theCreditCrunch.getCanonicalUri()));
        assertNotNull(items);
        assertEquals(1, items.size());
        assertTrue(items.get(0) instanceof Episode);
        
        Episode episode = (Episode) items.get(0);
        assertEquals(data.theCreditCrunch.getTitle(), episode.getTitle());
        
        Brand brandSummary = episode.getBrand();
        assertNotNull(brandSummary);
        assertEquals(data.dispatches.getCanonicalUri(), brandSummary.getCanonicalUri());
    }
    
    public void testShouldGetBrandOrPlaylist() throws Exception {
        store.createOrUpdatePlaylist(data.goodEastendersEpisodes, false);
        List<Playlist> playlists = store.findHydratedPlaylistsByCanonicalUri(Lists.newArrayList(data.goodEastendersEpisodes.getCanonicalUri()));
        assertEquals(1, playlists.size());
        assertFalse(playlists.get(0) instanceof Brand);
        
        store.createOrUpdatePlaylist(data.dispatches, false);
        playlists = store.findHydratedPlaylistsByCanonicalUri(Lists.newArrayList(data.dispatches.getCanonicalUri()));
        assertEquals(1, playlists.size());
        assertTrue(playlists.get(0) instanceof Brand);
    }
    
    public void testShouldGetEpisodeOrItem() throws Exception {
        store.createOrUpdateItem(data.englishForCats);
        List<Item> items = store.findItemsByCanonicalUri(Lists.newArrayList(data.englishForCats.getCanonicalUri()));
        assertEquals(1, items.size());
        assertFalse(items.get(0) instanceof Episode);
        
        store.createOrUpdateItem(data.brainSurgery);
        items = store.findItemsByCanonicalUri(Lists.newArrayList(data.brainSurgery.getCanonicalUri()));
        assertEquals(1, items.size());
        assertTrue(items.get(0) instanceof Episode);
    }
    
    public void testShouldGetEpisodeThroughAnonymousMethods() throws Exception {
        store.createOrUpdateItem(data.brainSurgery);
        
        Description episode = store.findByUri(data.brainSurgery.getCanonicalUri());
        assertNotNull(episode);
        assertTrue(episode instanceof Episode);
    }
    
    public void ignoreShouldPreserveAliases() throws Exception {
        data.theCreditCrunch.setAliases(Sets.newHashSet("somealias"));
        store.createOrUpdateItem(data.theCreditCrunch);
        
        data.theCreditCrunch.setAliases(Sets.newHashSet("anotheralias", "blah"));
        store.createOrUpdateItem(data.theCreditCrunch);
        
        List<Item> items = store.findItemsByCanonicalUri(Lists.newArrayList(data.theCreditCrunch.getCanonicalUri()));
        assertNotNull(items);
        assertEquals(1, items.size());
        assertEquals(3, items.get(0).getAliases().size());
    }
    
    public void ignoreShouldAddAliases() throws Exception {
        data.theCreditCrunch.setAliases(Sets.newHashSet("somealias"));
        store.createOrUpdateItem(data.theCreditCrunch);
        
        store.addAliases(data.theCreditCrunch.getCanonicalUri(), Sets.newHashSet("anotherAlias"));
        
        List<Item> items = store.findItemsByCanonicalUri(Lists.newArrayList(data.theCreditCrunch.getCanonicalUri()));
        assertNotNull(items);
        assertEquals(1, items.size());
        assertEquals(2, items.get(0).getAliases().size());
    }
    
    public void testShouldHaveContainedInUris() throws Exception {
        data.theCreditCrunch.setContainedIn(Sets.<Playlist>newHashSet(data.eastenders));
        assertNotNull(data.theCreditCrunch.getContainedInUris());
        assertEquals(2, data.theCreditCrunch.getContainedInUris().size());
        
        store.createOrUpdateItem(data.theCreditCrunch);
        
        List<Item> items = store.findItemsByCanonicalUri(Lists.newArrayList(data.theCreditCrunch.getCanonicalUri()));
        assertNotNull(items);
        assertEquals(1, items.size());
        assertEquals(2, items.get(0).getContainedInUris().size());
    }
    
    public void testShouldProcessSubPlaylists() throws Exception {
        store.createOrUpdatePlaylist(data.goodEastendersEpisodes, false);
        
        data.goodEastendersEpisodes.addPlaylist(data.neighbours);
        store.createOrUpdatePlaylist(data.goodEastendersEpisodes, false);
        
        List<Playlist> playlists = store.findHydratedPlaylistsByCanonicalUri(Lists.newArrayList(data.goodEastendersEpisodes.getCanonicalUri()));
        assertEquals(1, playlists.size());
        Collection<Playlist> subPlaylists = playlists.get(0).getPlaylists();
        assertEquals(1, subPlaylists.size());
        assertTrue(subPlaylists.iterator().next() instanceof Brand);
    }
    
    public void testShouldListAllItems() throws Exception {
        store.createOrUpdateItem(data.eggsForBreakfast);
        store.createOrUpdateItem(data.englishForCats);
        store.createOrUpdateItem(data.fossils);
        
        List<Item> items = ImmutableList.copyOf(store.listItems(null, 2));
        
        assertEquals(ImmutableList.of(data.fossils, data.englishForCats), items);
        
        items = ImmutableList.copyOf(store.listItems(data.englishForCats.getCanonicalUri(), 2));
        
        assertEquals(ImmutableList.of(data.eggsForBreakfast), items);
    }
    
    public void testShouldListAllPlaylists() throws Exception {
        store.createOrUpdatePlaylist(data.dispatches, false);
        store.createOrUpdatePlaylist(data.eastenders, false);
        
        List<Playlist> items = ImmutableList.copyOf(store.listPlaylists(null, 2));
        assertEquals(ImmutableList.of(data.eastenders, data.dispatches), items);
    }
    
    public void testThatItemsAreNotRemovedFromTheirBrands() throws Exception {
    	String itemUri = "itemUri";
    	String brandUri = "brandUri";

		Brand brand = new Brand(brandUri, "brand:curie", Publisher.BBC);
		brand.addItems(new Episode(itemUri, "item:curie", Publisher.BBC));
		
    	store.createOrUpdatePlaylist(brand, true);
    	
    	assertThat(store.findByUri(itemUri).getContainedInUris(), hasItem(brandUri)); 
    	assertThat(((Episode) store.findByUri(itemUri)).getBrand(), is(brand)); 
    	
    	store.createOrUpdateItem((new Item(itemUri, "item:curie", Publisher.BBC)));

    	assertThat(store.findByUri(itemUri).getContainedInUris(), hasItem(brandUri));
    	assertThat(((Episode) store.findByUri(itemUri)).getBrand(), is(brand)); 
    	
    	String playlistUri = "playlistUri";
		Playlist playlist = new Playlist(playlistUri, "playist:curie", Publisher.BBC);
    	playlist.addItem(new Item(itemUri, "item:curie", Publisher.BBC));
    	
    	store.createOrUpdatePlaylist(playlist, false);
    	assertThat(store.findByUri(itemUri).getContainedInUris(), hasItems(playlistUri, brandUri)); 
	}
    
    public void testItemsRemainInPlaylistsAfterUpdate() throws Exception {
    	String itemUri = "itemUri";
    	String playlistUri = "playlistUri";
    	
		Playlist playlist = new Playlist(playlistUri, "playlist:curie", Publisher.BBC);
		playlist.addItem(new Item(itemUri, "item:curie", Publisher.BBC));
		
		store.createOrUpdatePlaylist(playlist, false);
		
		assertThat(store.findByUri(itemUri).getContainedInUris(), hasItem(playlistUri));
		
		store.createOrUpdateItem(new Item(itemUri, "item:curie", Publisher.BBC));
		
		// the item should remain in the playlist
		assertThat(store.findByUri(itemUri).getContainedInUris(), hasItem(playlistUri));
		
		store.createOrUpdatePlaylist(new Playlist(playlistUri, "playlist:curie", Publisher.BBC), false);
		
		// it should now be removed from the playlist
		assertThat(store.findByUri(itemUri).getContainedInUris(), is(Collections.<String>emptySet()));
		
	}
    
    public void testBrandsRemainInPlaylistsAfterUpdate() throws Exception {
		String playlistUri = "playlistUri";
    	String brandUri = "brandUri";
    	
    	Playlist playlist = new Playlist(playlistUri, "playlist:curie", Publisher.BBC);
		playlist.addPlaylist(new Brand(brandUri, "brand:curie", Publisher.BBC));
		
		store.createOrUpdatePlaylist(playlist, false);
		
		assertThat(store.findByUri(brandUri).getContainedInUris(), hasItem(playlistUri));

		store.createOrUpdatePlaylist(new Brand(brandUri, "brand:curie", Publisher.BBC), true);
		
		assertThat(store.findByUri(brandUri).getContainedInUris(), hasItem(playlistUri));
		
		store.createOrUpdatePlaylist(new Playlist(playlistUri, "playlist:curie", Publisher.BBC), false);
		
		assertThat(store.findByUri(brandUri).getContainedInUris(), is(Collections.<String>emptySet()));

	}
    
    public void testThatSavingASkeletalPlaylistThatContainsSubElementsThatArentInTheDBThrowsAnException() throws Exception {
    	Item item = new Item("1", "1", Publisher.BLIP);
    	Item item2 = new Item("2", "2", Publisher.BLIP);

    	Playlist playlist = new Playlist();
		playlist.setCanonicalUri("playlist");
		playlist.addItems(item, item2, item2);
		
		try { 
			store.createOrUpdatePlaylistSkeleton(playlist);
			fail();
		} catch (IllegalArgumentException e) {
			// expected
		}
		
		store.createOrUpdateItem(item);
		store.createOrUpdateItem(item2);
		
		// should be ok now because the items are in the db
		store.createOrUpdatePlaylistSkeleton(playlist);
		
    	Playlist subplaylist = new Playlist("3", "3");

		playlist.addPlaylist(subplaylist);
		
		try { 
			store.createOrUpdatePlaylistSkeleton(playlist);
			fail();
		} catch (IllegalArgumentException e) {
			// expected
		}
	}
    
    public void testPersistingASkeletalPlaylist() throws Exception {
    	
    	Item item1 = new Item("i1", "1", Publisher.BLIP);
    	Item item2 = new Item("i2", "2", Publisher.BLIP);
    	Playlist subplaylist = new Playlist("subplaylist", "subplaylist");

    	store.createOrUpdateItem(item1);
		store.createOrUpdateItem(item2);
		store.createOrUpdatePlaylist(subplaylist, false);
    	
    	Playlist playlist = new Playlist();
		playlist.setCanonicalUri("playlist");
		playlist.addItems(item1, item2, item2);
		playlist.addPlaylist(subplaylist);
		
		store.createOrUpdatePlaylistSkeleton(playlist);
		
		Playlist found = (Playlist) store.findByUri("playlist");
		
		assertEquals(clock.now(), found.getLastFetched());
		assertEquals(clock.now(), found.getFirstSeen());
		
		assertEquals(2, found.getItems().size());
		assertEquals(1, found.getPlaylists().size());
		
		assertEquals(ImmutableSet.of("playlist"), store.findByUri("i1").getContainedInUris());
		assertEquals(ImmutableSet.of("playlist"), store.findByUri("i2").getContainedInUris());

		assertEquals(ImmutableSet.of("playlist"), store.findByUri("subplaylist").getContainedInUris());
	}
}
