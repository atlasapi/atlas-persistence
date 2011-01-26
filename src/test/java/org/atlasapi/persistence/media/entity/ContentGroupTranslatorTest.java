package org.atlasapi.persistence.media.entity;

import java.util.List;
import java.util.Set;

import junit.framework.TestCase;

import org.atlasapi.media.entity.ContentGroup;
import org.atlasapi.media.entity.Publisher;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.metabroadcast.common.time.SystemClock;
import com.mongodb.DBObject;

public class ContentGroupTranslatorTest extends TestCase {
	
    private final ContentGroupTranslator pt = new ContentGroupTranslator();
    
    @SuppressWarnings("unchecked")
    public void testFromPlaylist() throws Exception {
        ContentGroup playlist = new ContentGroup();
        playlist.setCanonicalUri("uri");
        playlist.setFirstSeen(new SystemClock().now());
        
        List<String> items = Lists.newArrayList();
        items.add("item");
        playlist.setContentUris(items);
        
        DBObject obj = pt.toDBObject(null, playlist);
        assertEquals("uri", obj.get(DescriptionTranslator.CANONICAL_URI));
        
        List<String> i = (List<String>) obj.get("contentUris");
        assertEquals(1, i.size());
        for (String item : i) {
            playlist.getContents().contains(item);
        }
    }
    
    public void testToPlaylist() throws Exception {
        ContentGroup playlist = new ContentGroup();
        playlist.setCanonicalUri("uri");
        playlist.setFirstSeen(new SystemClock().now());
        playlist.setDescription("description");
        playlist.setTitle("title");
        playlist.setPublisher(Publisher.BBC);
        
        Set<String> genres = Sets.newHashSet();
        genres.add("genre");
        playlist.setGenres(genres);
        
        Set<String> tags = Sets.newHashSet();
        tags.add("tag");
        playlist.setGenres(tags);
        
        List<String> playlists = Lists.newArrayList();
        playlists.add("playlist");
        playlist.setContentUris(playlists);
        
        List<String> items = Lists.newArrayList();
        items.add("item");
        playlist.setContentUris(items);
        
        DBObject obj = pt.toDBObject(null, playlist);
        
        ContentGroup p = pt.fromDBObject(obj, null);
        assertEquals(playlist.getCanonicalUri(), p.getCanonicalUri());
        assertEquals(playlist.getFirstSeen(), p.getFirstSeen());
        assertEquals(playlist.getDescription(), p.getDescription());
        assertEquals(playlist.getTitle(), p.getTitle());
        assertEquals(playlist.getPublisher(), p.getPublisher());
        assertEquals(playlist.getContentUris(), p.getContentUris());
        assertEquals(playlist.getGenres(), p.getGenres());
        assertEquals(playlist.getTags(), p.getTags());
        
        List<String> contentUris = playlist.getContentUris();
        assertEquals(1, contentUris.size());
        for (String item: contentUris) {
            assertTrue(items.contains(item));
        }
    }
}
