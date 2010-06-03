package org.uriplay.persistence.media.entity;

import java.util.List;
import java.util.Set;

import junit.framework.TestCase;

import org.uriplay.media.entity.Playlist;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.metabroadcast.common.time.SystemClock;
import com.mongodb.DBObject;

public class PlaylistTranslatorTest extends TestCase {
	
    PlaylistTranslator pt = new PlaylistTranslator(new ContentTranslator());
    
    @SuppressWarnings("unchecked")
    public void testFromPlaylist() throws Exception {
        Playlist playlist = new Playlist();
        playlist.setCanonicalUri("uri");
        playlist.setFirstSeen(new SystemClock().now());
        
        List<String> items = Lists.newArrayList();
        items.add("item");
        playlist.setItemUris(items);
        
        DBObject obj = pt.toDBObject(null, playlist);
        assertEquals("uri", obj.get(DescriptionTranslator.CANONICAL_URI));
        
        List<String> i = (List<String>) obj.get("itemUris");
        assertEquals(1, i.size());
        for (String item: i) {
            playlist.getItemUris().contains(item);
        }
    }
    
    public void testToPlaylist() throws Exception {
        Playlist playlist = new Playlist();
        playlist.setCanonicalUri("uri");
        playlist.setFirstSeen(new SystemClock().now());
        playlist.setDescription("description");
        playlist.setTitle("title");
        playlist.setPublisher("publisher");
        Set<String> containedInUris = Sets.newHashSet("uri");
        playlist.setContainedInUris(containedInUris);
        
        Set<String> genres = Sets.newHashSet();
        genres.add("genre");
        playlist.setGenres(genres);
        
        Set<String> tags = Sets.newHashSet();
        tags.add("tag");
        playlist.setGenres(tags);
        
        List<String> playlists = Lists.newArrayList();
        playlists.add("playlist");
        playlist.setItemUris(playlists);
        
        List<String> items = Lists.newArrayList();
        items.add("item");
        playlist.setItemUris(items);
        
        DBObject obj = pt.toDBObject(null, playlist);
        
        Playlist p = pt.fromDBObject(obj, null);
        assertEquals(playlist.getCanonicalUri(), p.getCanonicalUri());
        assertEquals(playlist.getFirstSeen(), p.getFirstSeen());
        assertEquals(playlist.getDescription(), p.getDescription());
        assertEquals(playlist.getTitle(), p.getTitle());
        assertEquals(playlist.getPublisher(), p.getPublisher());
        assertEquals(playlist.getContainedInUris(), p.getContainedInUris());
        assertEquals(playlist.getPlaylistUris(), p.getPlaylistUris());
        assertEquals(playlist.getGenres(), p.getGenres());
        assertEquals(playlist.getTags(), p.getTags());
        
        List<String> itemUris = playlist.getItemUris();
        assertEquals(1, itemUris.size());
        for (String item: itemUris) {
            assertTrue(items.contains(item));
        }
    }
}
