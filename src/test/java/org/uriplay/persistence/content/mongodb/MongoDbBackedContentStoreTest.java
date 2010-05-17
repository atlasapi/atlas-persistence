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

package org.uriplay.persistence.content.mongodb;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.uriplay.media.entity.Brand;
import org.uriplay.media.entity.Description;
import org.uriplay.media.entity.Encoding;
import org.uriplay.media.entity.Episode;
import org.uriplay.media.entity.Item;
import org.uriplay.media.entity.Location;
import org.uriplay.media.entity.Playlist;
import org.uriplay.media.entity.Version;
import org.uriplay.persistence.content.MongoDbBackedContentStore;
import org.uriplay.persistence.testing.DummyContentData;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.metabroadcast.common.query.Selection;

public class MongoDbBackedContentStoreTest extends BaseMongoDBTest {
	
	private MongoDbBackedContentStore store;
	private DummyContentData data ;
    
    @Override
    protected void setUp() throws Exception {
    	super.setUp();
    	this.store = new MongoDbBackedContentStore(mongo(), "uriplay");
    	data = new DummyContentData();
    }

    public void testShouldCreateAndRetrieveItem() throws Exception {
        data.eggsForBreakfast.setContainedIn(Sets.<Playlist> newHashSet(data.eastenders));
        store.createOrUpdateItem(data.eggsForBreakfast);
        store.createOrUpdateItem(data.englishForCats);

        List<Item> items = store.findItems(Lists.newArrayList(data.eggsForBreakfast.getCanonicalUri()));
        assertNotNull(items);
        assertEquals(1, items.size());
        assertEquals(data.eggsForBreakfast.getTitle(), items.get(0).getTitle());
        assertNotNull(items.get(0).getLastFetched());

        items = store.findItems(Lists.newArrayList(data.eggsForBreakfast.getCanonicalUri(), data.englishForCats.getCanonicalUri()));
        assertEquals(2, items.size());

        data.eggsForBreakfast.setCurie("some curie");
        store.createOrUpdateItem(data.eggsForBreakfast);

        items = store.findItems(Lists.newArrayList(data.eggsForBreakfast.getCanonicalUri()));
        assertNotNull(items);
        assertEquals(1, items.size());
        assertEquals(data.eggsForBreakfast.getTitle(), items.get(0).getTitle());
        assertEquals(data.eggsForBreakfast.getCurie(), items.get(0).getCurie());
        
        Set<String> containedInUris = items.get(0).getContainedInUris();
        assertEquals(2, containedInUris.size());
        assertEquals(data.eastenders.getCanonicalUri(), Iterables.get(containedInUris, 0));
        assertEquals(data.mentionedOnTwitter.getCanonicalUri(), Iterables.get(containedInUris, 1));
        
        
        Set<String> aliases = items.get(0).getAliases();
        for (String alias: aliases) {
            assertNotSame(items.get(0).getCanonicalUri(), alias);
        }
    }

    public void testShouldCreateAndRetrievePlaylist() throws Exception {
        store.createOrUpdatePlaylist(data.goodEastendersEpisodes, false);

        List<String> itemUris = Lists.newArrayList();
        for (Item item : data.goodEastendersEpisodes.getItems()) {
            itemUris.add(item.getCanonicalUri());
        }
        List<Item> items = store.findItems(itemUris);
        assertNotNull(items);
        assertEquals(1, items.size());
        assertEquals(data.dotCottonsBigAdventure.getTitle(), items.get(0).getTitle());

        data.goodEastendersEpisodes.setCurie("some curie");
        store.createOrUpdatePlaylist(data.goodEastendersEpisodes, false);

        List<Playlist> playlists = store.findPlaylists(Lists.newArrayList(data.goodEastendersEpisodes.getCanonicalUri()));
        assertNotNull(playlists);
        assertEquals(1, playlists.size());
        assertEquals(data.goodEastendersEpisodes.getTitle(), playlists.get(0).getTitle());
        assertEquals(data.goodEastendersEpisodes.getCurie(), playlists.get(0).getCurie());
        assertNotNull(playlists.get(0).getLastFetched());
        
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

    	Playlist beginningWithA = new Playlist();
    	beginningWithA.setCanonicalUri("/a");
    	beginningWithA.setPlaylists(Lists.<Playlist>newArrayList(data.eastenders));
    	
        store.createOrUpdatePlaylist(beginningWithA, true);
    	
        assertTrue(store.findPlaylists(Lists.newArrayList(data.eastenders.getCanonicalUri())).get(0).getContainedInUris().contains("/a"));
	}

    public void testShouldMarkUnavailableItemsAsUnavailable() throws Exception {
        data.goodEastendersEpisodes.addItem(data.eggsForBreakfast);
        store.createOrUpdatePlaylist(data.goodEastendersEpisodes, false);

        List<Playlist> playlists = store.findPlaylists(Lists.newArrayList(data.goodEastendersEpisodes.getCanonicalUri()));
        assertEquals(1, playlists.size());
        assertEquals(2, playlists.get(0).getItems().size());

        List<Item> newItems = Lists.newArrayList();
        newItems.add(data.eggsForBreakfast);
        newItems.add(data.englishForCats);
        data.goodEastendersEpisodes.setItems(newItems);
        assertEquals(2, data.goodEastendersEpisodes.getItems().size());
        assertEquals(2, data.goodEastendersEpisodes.getItemUris().size());

        store.createOrUpdatePlaylist(data.goodEastendersEpisodes, true);

        playlists = store.findPlaylists(Lists.newArrayList(data.goodEastendersEpisodes.getCanonicalUri()));
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

        playlists = store.findPlaylists(Lists.newArrayList(data.goodEastendersEpisodes.getCanonicalUri()));
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

        List<Item> items = store.findItems(Lists.newArrayList(data.theCreditCrunch.getCanonicalUri()));
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
        List<Playlist> playlists = store.findPlaylists(Lists.newArrayList(data.goodEastendersEpisodes.getCanonicalUri()));
        assertEquals(1, playlists.size());
        assertFalse(playlists.get(0) instanceof Brand);
        
        store.createOrUpdatePlaylist(data.dispatches, false);
        playlists = store.findPlaylists(Lists.newArrayList(data.dispatches.getCanonicalUri()));
        assertEquals(1, playlists.size());
        assertTrue(playlists.get(0) instanceof Brand);
    }
    
    public void testShouldGetEpisodeOrItem() throws Exception {
        store.createOrUpdateItem(data.englishForCats);
        List<Item> items = store.findItems(Lists.newArrayList(data.englishForCats.getCanonicalUri()));
        assertEquals(1, items.size());
        assertFalse(items.get(0) instanceof Episode);
        
        store.createOrUpdateItem(data.brainSurgery);
        items = store.findItems(Lists.newArrayList(data.brainSurgery.getCanonicalUri()));
        assertEquals(1, items.size());
        assertTrue(items.get(0) instanceof Episode);
    }
    
    public void testShouldGetEpisodeThroughAnonymousMethods() throws Exception {
        store.createOrUpdateGraph(Sets.newHashSet(data.brainSurgery), false);
        
        Description episode = store.findByUri(data.brainSurgery.getCanonicalUri());
        assertNotNull(episode);
        assertTrue(episode instanceof Episode);
    }
    
    public void ignoreShouldPreserveAliases() throws Exception {
        data.theCreditCrunch.setAliases(Sets.newHashSet("somealias"));
        store.createOrUpdateItem(data.theCreditCrunch);
        
        data.theCreditCrunch.setAliases(Sets.newHashSet("anotheralias", "blah"));
        store.createOrUpdateItem(data.theCreditCrunch);
        
        List<Item> items = store.findItems(Lists.newArrayList(data.theCreditCrunch.getCanonicalUri()));
        assertNotNull(items);
        assertEquals(1, items.size());
        assertEquals(3, items.get(0).getAliases().size());
    }
    
    public void ignoreShouldAddAliases() throws Exception {
        data.theCreditCrunch.setAliases(Sets.newHashSet("somealias"));
        store.createOrUpdateItem(data.theCreditCrunch);
        
        store.addAliases(data.theCreditCrunch.getCanonicalUri(), Sets.newHashSet("anotherAlias"));
        
        List<Item> items = store.findItems(Lists.newArrayList(data.theCreditCrunch.getCanonicalUri()));
        assertNotNull(items);
        assertEquals(1, items.size());
        assertEquals(2, items.get(0).getAliases().size());
    }
    
    public void testShouldHaveContainedInUris() throws Exception {
        data.theCreditCrunch.setContainedIn(Sets.<Playlist>newHashSet(data.eastenders));
        assertNotNull(data.theCreditCrunch.getContainedInUris());
        assertEquals(2, data.theCreditCrunch.getContainedInUris().size());
        
        store.createOrUpdateItem(data.theCreditCrunch);
        
        List<Item> items = store.findItems(Lists.newArrayList(data.theCreditCrunch.getCanonicalUri()));
        assertNotNull(items);
        assertEquals(1, items.size());
        assertEquals(2, items.get(0).getContainedInUris().size());
    }
    
    public void testShouldProcessSubPlaylists() throws Exception {
        store.createOrUpdatePlaylist(data.goodEastendersEpisodes, false);
        
        data.goodEastendersEpisodes.addPlaylist(data.neighbours);
        store.createOrUpdatePlaylist(data.goodEastendersEpisodes, false);
        
        List<Playlist> playlists = store.findPlaylists(Lists.newArrayList(data.goodEastendersEpisodes.getCanonicalUri()));
        assertEquals(1, playlists.size());
        Collection<Playlist> subPlaylists = playlists.get(0).getPlaylists();
        assertEquals(1, subPlaylists.size());
        assertTrue(subPlaylists.iterator().next() instanceof Brand);
    }
    
    public void testShouldListAllItems() throws Exception {
        store.createOrUpdateItem(data.eggsForBreakfast);
        store.createOrUpdateItem(data.englishForCats);
        store.createOrUpdateItem(data.everyoneNeedsAnEel);
        store.createOrUpdateItem(data.fossils);
        
        Selection selection = new Selection(0, 2);
        List<Item> items = store.listAllItems(selection);
        assertEquals(selection.getLimit().intValue(), items.size());
        
        selection = new Selection(2, 2);
        List<Item> nextItems = store.listAllItems(selection);
        assertEquals(selection.getLimit().intValue(), nextItems.size());
        
        for (Item item: items) {
            for (Item i: nextItems) {
                assertNotSame(item.getCanonicalUri(), i.getCanonicalUri());
            }
        }
    }
    
    public void testShouldNotListItemsAfterOffsex() throws Exception {
        Selection selection = new Selection(1000000, 2);
        List<Item> items = store.listAllItems(selection);
        assertEquals(0, items.size());
    }
    
    public void testShouldListAllPlaylists() throws Exception {
        store.createOrUpdatePlaylist(data.goodEastendersEpisodes, false);
        store.createOrUpdatePlaylist(data.apprentice, false);
        store.createOrUpdatePlaylist(data.dispatches, false);
        store.createOrUpdatePlaylist(data.eastenders, false);
        
        Selection selection = new Selection(0, 2);
        List<Playlist> playlists = store.listAllPlaylists(selection);
        assertEquals(selection.getLimit().intValue(), playlists.size());
        
        selection = new Selection(2, 2);
        List<Playlist> nextPlaylists = store.listAllPlaylists(selection);
        assertEquals(selection.getLimit().intValue(), nextPlaylists.size());
        
        for (Playlist playlist: playlists) {
            for (Playlist p: nextPlaylists) {
                assertNotSame(playlist.getCanonicalUri(), p.getCanonicalUri());
            }
        }
    }
}
