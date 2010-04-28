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

package org.uriplay.persistence.content;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.time.StopWatch;
import org.jherd.util.Selection;
import org.joda.time.DateTime;
import org.uriplay.content.criteria.ContentQuery;
import org.uriplay.media.entity.Brand;
import org.uriplay.media.entity.Description;
import org.uriplay.media.entity.Encoding;
import org.uriplay.media.entity.Episode;
import org.uriplay.media.entity.Item;
import org.uriplay.media.entity.Location;
import org.uriplay.media.entity.Playlist;
import org.uriplay.media.entity.Version;
import org.uriplay.media.util.ChildFinder;
import org.uriplay.persistence.content.mongodb.MongoDBQueryBuilder;
import org.uriplay.persistence.content.mongodb.MongoDBTemplate;

import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

public class MongoDbBackedContentStore extends MongoDBTemplate implements MutableContentStore {
    
	private final static int DEFAULT_BATCH_SIZE = 50;
    private final DBCollection itemCollection;
    private final DBCollection playlistCollection;

    public MongoDbBackedContentStore(Mongo mongo, String dbName) {
    	super(mongo, dbName);
        itemCollection = table("items");
        playlistCollection = table("playlists");
    }

	@Override
    public void addAliases(String uri, Set<String> aliases) {
        Description desc = findByUri(uri);
        if (desc != null) {
            for (String alias : aliases) {
                desc.addAlias(alias);
            }
            createOrUpdateGraph(Sets.newHashSet(desc), false);
        }
        
    }

    @Override
    public void createOrUpdateGraph(Set<?> beans, boolean markMissingItemsAsUnavailable) {
        Iterable<?> roots = rootsOf(beans);

        StopWatch timer = new StopWatch();
        timer.start();

        for (Object root : roots) {
            if (root instanceof Playlist) {
                createOrUpdatePlaylist((Playlist) root, markMissingItemsAsUnavailable);
            }
            if (root instanceof Item) {
                createOrUpdateItem((Item) root);
            }
        }
        timer.stop();
    }

    private Iterable<?> rootsOf(Set<?> beans) {
        return Iterables.filter(beans, Predicates.not(new ChildFinder(beans)));
    }

    @Override
    public void createOrUpdateItem(Item item) {
        try {
            Item oldItem = null;
            List<Item> items = findItems(Lists.newArrayList(item.getCanonicalUri()));
            if (!items.isEmpty()) {
                oldItem = items.get(0);
                preserveAliases(item, oldItem);
            }

            addUriToAliases(item);
            if (oldItem == null) {
                item.setFirstSeen(new DateTime());
            }
            item.setLastFetched(new DateTime());

            DBObject query = new BasicDBObject();
            query.put("canonicalUri", item.getCanonicalUri());

            itemCollection.update(query, formatForDB(item), true, false);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

	@Override
    public void createOrUpdatePlaylist(Playlist playlist, boolean markMissingItemsAsUnavailable) {
        try {
            Playlist oldPlaylist = null;
            List<Playlist> playlists = findPlaylists(Lists.newArrayList(playlist.getCanonicalUri()));
            if (!playlists.isEmpty()) {
                oldPlaylist = playlists.get(0);
                preserveAliases(playlist, oldPlaylist);
            }

            if (markMissingItemsAsUnavailable && oldPlaylist != null) {
                Set<String> oldUris = Sets.difference(Sets.newHashSet(oldPlaylist.getItemUris()), Sets.newHashSet(playlist.getItemUris()));

                for (Item item : findItems(Lists.newArrayList(oldUris))) {
                    for (Version version : item.getVersions()) {
                        for (Encoding encoding : version.getManifestedAs()) {
                            for (Location location : encoding.getAvailableAt()) {
                                location.setAvailable(false);
                            }
                        }
                    }
                    playlist.addItem(item);
                }
            }

            for (Item item : playlist.getItems()) {
                createOrUpdateItem(item);
            }

            for (Playlist subPlaylist : playlist.getPlaylists()) {
                createOrUpdatePlaylist(subPlaylist, markMissingItemsAsUnavailable);
            }

            addUriToAliases(playlist);
            if (oldPlaylist == null) {
                playlist.setFirstSeen(new DateTime());
            }
            playlist.setLastFetched(new DateTime());

            DBObject query = new BasicDBObject();
            query.put("canonicalUri", playlist.getCanonicalUri());

            playlistCollection.update(query, formatForDB(playlist), true, false);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void preserveAliases(Description newDesc, Description oldDesc) {
        Set<String> oldAliases = Sets.difference(oldDesc.getAliases(), newDesc.getAliases());

        for (String alias : oldAliases) {
            newDesc.addAlias(alias);
        }
    }

    private void addUriToAliases(Description desc) {
        desc.addAlias(desc.getCanonicalUri());
    }

    private void removeUriFromAliases(Description desc) {
        desc.getAliases().remove(desc.getCanonicalUri());
    }

    @Override
    public Description findByUri(String uri) {
        List<Item> items = findItems(Lists.newArrayList(uri));
        if (!items.isEmpty()) {
            return items.get(0);
        }
        List<Playlist> playlists = findPlaylists(Lists.newArrayList(uri));
        if (!playlists.isEmpty()) {
            return playlists.get(0);
        }
        return null;
    }

    @Override
    public List<Item> findItems(List<String> uris) {
        return executeItemQuery(in("aliases", uris), null);
    }

    private List<Item> executeItemQuery(DBObject query, Selection selection) {
    	
        Iterator<DBObject> cur = cursor(itemCollection, query, selection);
        
        if (cur == null) {
        	return Collections.emptyList();
        }
        
        List<Item> items = Lists.newArrayList();
        while (cur.hasNext()) {
            DBObject current = cur.next();
            items.add(toItem(current));
        }

        return items;
    }

    private Iterator<DBObject> cursor(DBCollection collection, DBObject query, Selection selection) {
        if (selection != null && (selection.hasLimit() || selection.hasStartIndex())) {
            return collection.find(query, new BasicDBObject(), selection.startIndexOrDefaultValue(0), hardLimit(selection));
        } else {
            return collection.find(query, new BasicDBObject(), 0, DEFAULT_BATCH_SIZE);
        }
    }

    private Integer hardLimit(Selection selection) {
        return -1 * selection.limitOrDefaultValue(0);
    }

    private MongoDBQueryBuilder queryBuilder = new MongoDBQueryBuilder();

    public List<Item> itemsMatching(ContentQuery query) {
        return executeItemQuery(queryBuilder.buildItemQuery(query), query.getSelection());
    }
    
	@SuppressWarnings("unchecked")
	public List<Brand> dehydratedBrandsMatching(ContentQuery query) {
		return (List) executePlaylistQuery(queryBuilder.buildBrandQuery(query), Brand.class.getSimpleName(), query.getSelection(), false);
	}    
	
	@SuppressWarnings("unchecked")
	public List<Playlist> dehydratedPlaylistsMatching(ContentQuery query) {
		return (List) executePlaylistQuery(queryBuilder.buildPlaylistQuery(query), null, query.getSelection(), false);
	}    

    @Override
    public List<Playlist> findPlaylists(List<String> uris) {
        return executePlaylistQuery(in("aliases", uris), null, null, true);
    }

	private List<Playlist> executePlaylistQuery(DBObject query, String type, Selection selection, boolean hydrate) {
        List<Playlist> playlists = Lists.newArrayList();
        try {
        	
        	if (type != null) {
        		query.put("type", type);
        	}
            Iterator<DBObject> cur = cursor(playlistCollection, query, selection);
            if (cur == null) {
            	return Collections.emptyList();
            }
            
            while (cur.hasNext()) {
                DBObject current = cur.next();
                playlists.add(toPlaylist(current, hydrate));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return playlists;
    }

    @Override
    public List<Item> listAllItems(Selection selection) {
        List<Item> items = Lists.newArrayList();

        Iterator<DBObject> objects = itemCollection.find(null, null, selection.startIndexOrDefaultValue(0), -1 * selection.limitOrDefaultValue(DEFAULT_BATCH_SIZE));
        while (objects != null && objects.hasNext()) {
            DBObject current = objects.next();
            items.add(toItem(current));
        }

        return items;
    }

    @Override
    public List<Playlist> listAllPlaylists(Selection selection) {
        List<Playlist> playlists = Lists.newArrayList();

        Iterator<DBObject> objects = playlistCollection.find(null, null, selection.startIndexOrDefaultValue(0), -1 * selection.limitOrDefaultValue(DEFAULT_BATCH_SIZE));
        while (objects != null && objects.hasNext()) {
            DBObject current = objects.next();
            playlists.add(toPlaylist(current, true));
        }

        return playlists;
    }

    private Item toItem(DBObject object) {
        Item item = null;

        try {
            if (object.containsField("type") && Episode.class.getSimpleName().equals(object.get("type"))) {
                item = fromDb(object, Episode.class);
            } else {
                item = fromDb(object, Item.class);
            }

            removeUriFromAliases(item);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return item;
    }

    private Playlist toPlaylist(DBObject object, boolean hydrate) {
        Playlist playlist = null;
        
        try {
            if (object.containsField("type") && Brand.class.getSimpleName().equals(object.get("type"))) {
                playlist = fromDb(object, Brand.class);
            } else {
                playlist = fromDb(object, Playlist.class);
            }

            if (hydrate) {
            	List<Item> items = findItems(Lists.newArrayList(playlist.getItemUris()));
            	playlist.setItems(items);

            	List<Playlist> subPlaylists = findPlaylists(Lists.newArrayList(playlist.getPlaylistUris()));
            	playlist.setPlaylists(subPlaylists);
            }
            
            removeUriFromAliases(playlist);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return playlist;
    }
}
