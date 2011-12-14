package org.atlasapi.persistence.media.entity;

import java.util.Set;

import junit.framework.TestCase;

import org.atlasapi.media.entity.ContentGroup;
import org.atlasapi.media.entity.Person;
import org.atlasapi.media.entity.Publisher;

import com.google.common.collect.Sets;
import com.metabroadcast.common.time.SystemClock;
import com.mongodb.DBObject;

public class ContentGroupTranslatorTest extends TestCase {
	
    private final PersonTranslator pt = new PersonTranslator();
    
    public void testFromPlaylist() throws Exception {
        Person playlist = new Person();
        playlist.setCanonicalUri("uri");
        playlist.setFirstSeen(new SystemClock().now());
        
        
        DBObject obj = pt.toDBObject(null, playlist);
        assertEquals("uri", obj.get(IdentifiedTranslator.CANONICAL_URI));
    }
    
    public void testToPlaylist() throws Exception {
        Person playlist = new Person();
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
        
        DBObject obj = pt.toDBObject(null, playlist);
        
        ContentGroup p = pt.fromDBObject(obj, null);
        assertEquals(playlist.getCanonicalUri(), p.getCanonicalUri());
        assertEquals(playlist.getFirstSeen(), p.getFirstSeen());
        assertEquals(playlist.getDescription(), p.getDescription());
        assertEquals(playlist.getTitle(), p.getTitle());
        assertEquals(playlist.getPublisher(), p.getPublisher());
        assertEquals(playlist.getContents(), p.getContents());
        assertEquals(playlist.getGenres(), p.getGenres());
        assertEquals(playlist.getTags(), p.getTags());
    }
}
