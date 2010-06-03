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

import static com.metabroadcast.common.persistence.mongo.MongoBuilders.update;
import static com.metabroadcast.common.persistence.mongo.MongoBuilders.where;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.joda.time.DateTime;
import org.uriplay.content.criteria.ContentQuery;
import org.uriplay.media.entity.Brand;
import org.uriplay.media.entity.Content;
import org.uriplay.media.entity.Description;
import org.uriplay.media.entity.Encoding;
import org.uriplay.media.entity.Episode;
import org.uriplay.media.entity.Item;
import org.uriplay.media.entity.Location;
import org.uriplay.media.entity.Playlist;
import org.uriplay.media.entity.Version;
import org.uriplay.persistence.content.mongodb.MongoDBQueryBuilder;
import org.uriplay.persistence.content.mongodb.MongoDBTemplate;
import org.uriplay.persistence.media.entity.ContentTranslator;
import org.uriplay.persistence.media.entity.DescriptionTranslator;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.metabroadcast.common.persistence.mongo.MongoQueryBuilder;
import com.metabroadcast.common.persistence.mongo.MongoUpdateBuilder;
import com.metabroadcast.common.query.Selection;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

public class MongoDbBackedContentStore extends MongoDBTemplate implements MutableContentStore {
    
	private final static int DEFAULT_BATCH_SIZE = 50;
	private final static int MAX_RESULTS = 2000;
	
    private final DBCollection itemCollection;
    private final DBCollection playlistCollection;

    public MongoDbBackedContentStore(Mongo mongo, String dbName) {
    	super(mongo, dbName);
        itemCollection = table("items");
        playlistCollection = table("playlists");
    }

	@Override
    public void addAliases(String uri, Set<String> aliases) {
        Content desc = findByUri(uri);
        if (desc != null) {
            for (String alias : aliases) {
                desc.addAlias(alias);
            }
            createOrUpdateContent(desc, false);
        }
    }

    @Override
    public void createOrUpdateContent(Content root, boolean markMissingItemsAsUnavailable) {
        if (root instanceof Playlist) {
            createOrUpdatePlaylist((Playlist) root, markMissingItemsAsUnavailable);
        }
        if (root instanceof Item) {
            createOrUpdateItem((Item) root);
        }
    }
    
    @Override
    public void createOrUpdateItem(Item item) {
    	createOrUpdateItem(item, false);
    }

    private void createOrUpdateItem(Item item, boolean markMissingItemsAsUnavailable) {
        try {
            Content content = findByUri(item.getCanonicalUri());
            if (content != null) {
            	if (! (content instanceof Item)) {
            		throw new IllegalArgumentException("Cannot update item with uri: " + item.getCanonicalUri() +  "  since the old entity was not an item");
            	}
                Item oldItem = (Item) content;

                preserveContainedIn(item, oldItem);
                
                if (!markMissingItemsAsUnavailable && oldItem instanceof Episode) {
                	writeContainedIn(itemCollection, item, item.getContainedInUris());
                	return;
                }
                
                preserveAliases(item, oldItem);
            } else {
            	item.setFirstSeen(new DateTime());
            }

            addUriAndCurieToAliases(item);
            item.setLastFetched(new DateTime());

            DBObject query = new BasicDBObject();
            query.put(DescriptionTranslator.CANONICAL_URI, item.getCanonicalUri());

            itemCollection.update(query, toDB(item), true, false);
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

            if (oldPlaylist != null) {
            	Set<String> oldItemUris = Sets.difference(Sets.newHashSet(oldPlaylist.getItemUris()), Sets.newHashSet(playlist.getItemUris()));
            	List<Item> oldItems = findItems(Lists.newArrayList(oldItemUris));
	            if (markMissingItemsAsUnavailable) {
					for (Item item : oldItems) {
	                    for (Version version : item.getVersions()) {
	                        for (Encoding encoding : version.getManifestedAs()) {
	                            for (Location location : encoding.getAvailableAt()) {
	                                location.setAvailable(false);
	                            }
	                        }
	                    }
	                    playlist.addItem(item);
	                }
	            } else {
	            	for (Item item : oldItems) {
						removeContainedIn(itemCollection, item, playlist.getCanonicalUri());
					}
	            }
	            
	            preserveContainedIn(playlist, oldPlaylist);
	            
            	Set<String> oldPlaylistUris = Sets.difference(Sets.newHashSet(oldPlaylist.getPlaylistUris()), Sets.newHashSet(playlist.getPlaylistUris()));
            	List<Playlist> oldPlaylists = findPlaylists(Lists.newArrayList(oldPlaylistUris));

	            for (Playlist oldSubPlaylist : oldPlaylists) {
					removeContainedIn(playlistCollection, oldSubPlaylist, playlist.getCanonicalUri());
				}
            }

            for (Item item : playlist.getItems()) {
                createOrUpdateItem(item, markMissingItemsAsUnavailable);
            }

            for (Playlist subPlaylist : playlist.getPlaylists()) {
                createOrUpdatePlaylist(subPlaylist, markMissingItemsAsUnavailable);
            }

            addUriAndCurieToAliases(playlist);
            if (oldPlaylist == null) {
                playlist.setFirstSeen(new DateTime());
            }
            playlist.setLastFetched(new DateTime());

            DBObject query = new BasicDBObject();
            query.put(DescriptionTranslator.CANONICAL_URI, playlist.getCanonicalUri());

            playlistCollection.update(query, toDB(playlist), true, false);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
	
    private void removeContainedIn(DBCollection collection, Content content, String containedInUri) {
    	collection.update(new BasicDBObject(DescriptionTranslator.CANONICAL_URI, content.getCanonicalUri()), new BasicDBObject("$pull", new BasicDBObject(ContentTranslator.CONTAINED_IN_URIS_KEY, containedInUri)));
    }
    
    private void writeContainedIn(DBCollection collection, Content content, Set<String> containedInUris) {
		MongoQueryBuilder findByUri = where().fieldEquals(DescriptionTranslator.CANONICAL_URI, content.getCanonicalUri());
		MongoUpdateBuilder update = update().setField(ContentTranslator.CONTAINED_IN_URIS_KEY, containedInUris);
		collection.update(findByUri.build(), update.build());
    }

	private void preserveAliases(Description newDesc, Description oldDesc) {
        Set<String> oldAliases = Sets.difference(oldDesc.getAliases(), newDesc.getAliases());

        for (String alias : oldAliases) {
            newDesc.addAlias(alias);
        }
    }
	
	private void preserveContainedIn(Content newDesc, Content oldDesc) {
		newDesc.setContainedInUris(Sets.newHashSet(Sets.union(oldDesc.getContainedInUris(), newDesc.getContainedInUris())));
	}

    private void addUriAndCurieToAliases(Description desc) {
        desc.addAlias(desc.getCanonicalUri());
        desc.addAlias(desc.getCurie());
    }

    private void removeUriFromAliases(Description desc) {
        desc.getAliases().remove(desc.getCanonicalUri());
        desc.getAliases().remove(desc.getCurie());
    }

    @Override
    public Content findByUri(String uri) {
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
        int loaded = 0;
        List<Item> items = Lists.newArrayList();
        while (cur.hasNext()) {
            DBObject current = cur.next();
            items.add(toItem(current));
            loaded++;
            if (loaded > MAX_RESULTS) {
            	throw new IllegalArgumentException("Too many results for query");
            }
        }

        return items;
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
            int loaded = 0;
            while (cur.hasNext()) {
                DBObject current = cur.next();
                playlists.add(toPlaylist(current, hydrate));
                loaded++;
                if (loaded > MAX_RESULTS) {
                	throw new IllegalArgumentException("Too many results for query");
                }
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
                item = fromDB(object, Episode.class);
            } else {
                item = fromDB(object, Item.class);
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
                playlist = fromDB(object, Brand.class);
            } else {
                playlist = fromDB(object, Playlist.class);
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
