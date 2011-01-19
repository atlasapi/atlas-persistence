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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.metabroadcast.common.persistence.mongo.MongoBuilders.update;
import static com.metabroadcast.common.persistence.mongo.MongoBuilders.where;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Clip;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.ContentGroup;
import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.Encoding;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Location;
import org.atlasapi.media.entity.Series;
import org.atlasapi.media.entity.Version;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.RetrospectiveContentLister;
import org.atlasapi.persistence.media.entity.DescriptionTranslator;
import org.joda.time.DateTime;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.persistence.mongo.MongoQueryBuilder;
import com.metabroadcast.common.query.Selection;
import com.metabroadcast.common.time.Clock;
import com.metabroadcast.common.time.DateTimeZones;
import com.metabroadcast.common.time.SystemClock;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class MongoDbBackedContentStore extends MongoDBTemplate implements ContentWriter, ContentResolver, RetrospectiveContentLister, AliasWriter {

    private final static int MAX_RESULTS = 20000;

    private static final Log LOG = LogFactory.getLog(MongoDbBackedContentStore.class);


    private final Clock clock;
    private final DBCollection contentCollection;
    private final DBCollection contentGroupCollection;
	private final DBCollection aliasesCollection;

    public MongoDbBackedContentStore(DatabasedMongo mongo, Clock clock) {
        super(mongo);
		this.clock = clock;
        contentCollection = table("content");
        contentGroupCollection = table("groups");
        aliasesCollection = table("aliases");
    }

    public MongoDbBackedContentStore(DatabasedMongo mongo) {
    	this(mongo, new SystemClock());
	}

	@Override
    public void addAliases(String uri, Set<String> aliases) {
    	boolean wasItem = addAliasesTo(uri, aliases, contentCollection);
		if (!wasItem) {
			addAliasesTo(uri, aliases, contentGroupCollection);
		}
    }

	@SuppressWarnings("unchecked")
	private boolean addAliasesTo(String uri, Set<String> aliases, DBCollection collection) {
		MongoQueryBuilder findByCanonicalUri = findByCanonicalUriQuery(uri);
		Iterable<DBObject> found = findByCanonicalUri.find(collection);
		if (!Iterables.isEmpty(found)) {
			DBObject dbo = Iterables.getOnlyElement(found);
			Set<String> oldAliases = ImmutableSet.copyOf(((Iterable<String>) dbo.get(DescriptionTranslator.ALIASES)));
			collection.update(findByCanonicalUri.build(), update().setField(DescriptionTranslator.ALIASES, Sets.union(aliases, oldAliases)).build(), false, false);
			return true;
		}
		return false;
	}

	private MongoQueryBuilder findByCanonicalUriQuery(String uri) {
		return where().fieldEquals(DescriptionTranslator.CANONICAL_URI, uri);
	}

    @Override
    public void createOrUpdate(Item item) {
        createOrUpdateItem(item, null);
    }

    @SuppressWarnings("unchecked")
	private void createOrUpdateItem(Item item, Container<?> parent) {
        try {
            Identified content = findByCanonicalUri(item.getCanonicalUri());
            if (content != null) {
                if (!(content instanceof Item)) {
                    throw new IllegalArgumentException("Cannot update item with uri: " + item.getCanonicalUri()
                                    + "  since the old entity was not an item");
                }
                Item oldItem = (Item) content;

                if (oldItem.getContainer() != null && item.getContainer() == null) {
                	// don't update the item if doing so would remove it from its container
                	return;
                }
                preserveAliases(item, oldItem);
            } else {
                item.setFirstSeen(clock.now());
            }
            if (parent == null) {
            	Container<?> brand = item.getContainer();
				if (brand != null) {
            		Identified dbContent = findByCanonicalUri(brand.getCanonicalUri());
            		if (dbContent instanceof Container<?>) {
            			Container<Item> dbBrand = (Container<Item>) dbContent;
            			if (!dbBrand.getContentUris().contains(item.getCanonicalUri())) {
            				dbBrand.addContents(item);
            				updateBasicPlaylistDetails(dbBrand, contentCollection);
            			} else {
            				dbBrand.addContents(item);
            			}
            		} 
            	}
            }

            item.setLastFetched(new DateTime());
            setThisOrChildLastUpdated(item);
            
            DBObject query = new BasicDBObject();
            query.put(DescriptionTranslator.CANONICAL_URI, item.getCanonicalUri());
            contentCollection.update(query, toDB(item), true, false);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    @Override
    @SuppressWarnings("unchecked")
    public void createOrUpdate(Container<?> container, boolean markMissingItemsAsUnavailable) {
        try {
			Identified oldContent = findByCanonicalUri(container.getCanonicalUri());

			if (oldContent != null) {
				if (!(oldContent instanceof Container<?>)) {
					throw new IllegalStateException("Cannot save container " + container.getCanonicalUri() + " because there's already an item with that uri");
				}
				Container<?> oldPlaylist = (Container<Item>) oldContent;
            	
            	Set<String> oldItemUris = Sets.difference(ImmutableSet.copyOf(oldPlaylist.getContainedInUris()), ImmutableSet.copyOf(container.getContainedInUris()));
                
            	List<Item> oldItems = (List) findByCanonicalUri(Lists.newArrayList(oldItemUris));
                if (markMissingItemsAsUnavailable) {
                    for (Item item : oldItems) {
                        for (Version version : item.getVersions()) {
                            for (Encoding encoding : version.getManifestedAs()) {
                                for (Location location : encoding.getAvailableAt()) {
                                    location.setAvailable(false);
                                    location.setLastUpdated(new DateTime(DateTimeZones.UTC));
                                }
                            }
                        }
                        ((Container<Item>) container).addContents(item);
                    }
                } 
            	preservePlaylistAttributes(container, oldPlaylist);
            }

            for (Item item : container.getContents()) {
             //   createOrUpdateItem(item, container);
            }


            if (oldContent == null) {
                container.setFirstSeen(new DateTime());
            }
            
            container.setLastFetched(new DateTime());
            setThisOrChildLastUpdated(container);

            updateBasicPlaylistDetails(container, contentCollection);
            
//            if (container instanceof Brand) {
//            	Brand brand = (Brand) container;
//            	Set<Series> series = fullSeriesFrom(brand);
//            	for (Series sery : series) {
//            		createOrUpdateSkeleton(sery);
//				}
//            }
            
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

	private void preservePlaylistAttributes(Identified playlist, Identified oldPlaylist) {
		preserveAliases(playlist, oldPlaylist);
	}
    
    @Override
	public void createOrUpdateSkeleton(ContentGroup playlist) {
    	checkNotNull(playlist.getCanonicalUri(), "Cannot persist a playlist without a canonical uri");

    	checkArgument(checkThatSubElementsExist(playlist.getContents(), contentCollection), "Not all content exists in the database for playlist: " + playlist.getCanonicalUri());
    	
    	Identified previousValue = findByCanonicalUri(playlist.getCanonicalUri());
    	
    	if (previousValue != null) {
    		preservePlaylistAttributes(playlist, (ContentGroup) previousValue);
    	}
    	
    	DateTime now = clock.now();
    	
        if (previousValue == null) {
            playlist.setFirstSeen(now);
        }
        
        playlist.setLastFetched(now);
        updateBasicPlaylistDetails(playlist, contentGroupCollection);
	}

	private boolean checkThatSubElementsExist(List<? extends Content> content, DBCollection collection) {
		ImmutableSet<String> uris = ImmutableSet.copyOf(Iterables.transform(content, Identified.TO_URI));
		return uris.size() == findByCanonicalUri(uris).size();
	}

	private void updateBasicPlaylistDetails(Described playlist, DBCollection collection) {
		DBObject query = new BasicDBObject();
		query.put(DescriptionTranslator.CANONICAL_URI, playlist.getCanonicalUri());
		collection.update(query, toDB(playlist), true, false);
	}
    
    private DateTime setThisOrChildLastUpdated(Container<?> playlist) {
        DateTime thisOrChildLastUpdated = thisOrChildLastUpdated(null, playlist.getLastUpdated());
        for (Item item: playlist.getContents()) {
            thisOrChildLastUpdated = thisOrChildLastUpdated(thisOrChildLastUpdated, setThisOrChildLastUpdated(item));
        }
        playlist.setThisOrChildLastUpdated(thisOrChildLastUpdated);
        return thisOrChildLastUpdated;
    }
    
    private DateTime setThisOrChildLastUpdated(Item item) {
        DateTime thisOrChildLastUpdated = thisOrChildLastUpdated(null, item.getLastUpdated());
        
        for (Version version: item.getVersions()) {
            thisOrChildLastUpdated = thisOrChildLastUpdated(thisOrChildLastUpdated, version.getLastUpdated());
            
            for (Broadcast broadcast: version.getBroadcasts()) {
                thisOrChildLastUpdated = thisOrChildLastUpdated(thisOrChildLastUpdated, broadcast.getLastUpdated());
            }
            
            for (Encoding encoding: version.getManifestedAs()) {
                thisOrChildLastUpdated = thisOrChildLastUpdated(thisOrChildLastUpdated, encoding.getLastUpdated());
                
                for (Location location: encoding.getAvailableAt()) {
                    thisOrChildLastUpdated = thisOrChildLastUpdated(thisOrChildLastUpdated, location.getLastUpdated());
                }
            }
        }
        
        return thisOrChildLastUpdated;
    }
    
    private DateTime thisOrChildLastUpdated(DateTime current, DateTime candidate) {
        if (candidate != null && (current == null || candidate.isAfter(current))) {
            return candidate;
        }
        return current;
    }

    private Set<Series> fullSeriesFrom(Container<?> brand) {
    	Set<Series> series = Sets.newHashSet();
    	for (Item item : brand.getContents()) {
			if (item instanceof Episode) {
				Episode episode = (Episode) item;
				Series sery = episode.getHydratedSeries();
				if (sery != null) {
					series.add(sery);
				}
			}
		}
    	return series;
    }

    private void preserveAliases(Identified newDesc, Identified oldDesc) {
        Set<String> oldAliases = Sets.difference(oldDesc.getAliases(), newDesc.getAliases());

        for (String alias : oldAliases) {
            newDesc.addAlias(alias);
        }
    }

    @Override
    public Identified findByCanonicalUri(String uri) {
        return Iterables.getOnlyElement(findByCanonicalUri(ImmutableList.of(uri)), null);
    }
    
	public List<? extends Identified> findByUriOrAlias(Iterable<String> uris) {
		final ImmutableSet<String> uriSet = ImmutableSet.copyOf(uris);
		
       	Function<Identified, Identified> extractItem = new Function<Identified, Identified>() {

			@Override
			public Identified apply(Identified content) {
				return extactItemIfInternalUrl(content, uriSet);
			}
       		
       	};
		return ImmutableList.copyOf(Iterables.transform(where().fieldIn(DescriptionTranslator.LOOKUP, uris).find(contentCollection), Functions.compose(extractItem, TO_MODEL)));
	}

	private <T extends Identified> Iterable<T> extractCanonical(final Set<String> uris, Iterable<T> elems) {
	    return Iterables.filter(elems, new Predicate<Identified>() {

			@Override
			public boolean apply(Identified input) {
				return uris.contains(input.getCanonicalUri()) || uris.contains(input.getCurie());
			}
	    });
	}
	
	public List<Identified> findByCanonicalUri(final Iterable<String> uris) {
		return ImmutableList.copyOf(extractCanonical(ImmutableSet.copyOf(uris), findByUriOrAlias(uris)));
    }
	
	private static Identified extactItemIfInternalUrl(Identified content, Set<String> uris) {
		if (!(content instanceof Container<?>)) {
			return content; 
		}
		if (!Sets.intersection(DescriptionTranslator.lookupElemsFor(content), uris).isEmpty()) {
			return content;
		}
		Container<?> container = (Container<?>) content;
		for (Item item : container.getContents()) {
			if (!Sets.intersection(DescriptionTranslator.lookupElemsFor(item), uris).isEmpty()) {
				return item;
			}
		}
		throw new IllegalStateException();
	}

    List<Content> executeDiscoverQuery(DBObject query, Selection selection) {

        Iterator<DBObject> cur = cursor(contentCollection, query, selection);

        if (cur == null) {
            return Collections.emptyList();
        }
        int loaded = 0;
        List<Content> items = Lists.newArrayList();
        try {
            while (cur.hasNext()) {
                DBObject current = cur.next();
                items.add((Content) toModel(current));
                loaded++;
                if (loaded > MAX_RESULTS) {
                    throw new IllegalArgumentException("Too many results for query");
                }
            }
        } catch (IllegalArgumentException e) {
            LOG.error("IllegalArguementThrown: " + e.getMessage() + ". Query was: " + query + ", and Selection: " + selection);
            throw e;
        }

        return items;
    }

    private MongoDBQueryBuilder queryBuilder = new MongoDBQueryBuilder();

//    public List<Playlist> dehydratedPlaylistsMatching(ContentQuery query) {
//        return executePlaylistQuery(queryBuilder.buildPlaylistQuery(query), null, query.getSelection(), false);
//    }

//    List<Playlist> findHydratedPlaylistsByCanonicalUri(Iterable<String> uris) {
//        return executePlaylistQuery(where().fieldIn(DescriptionTranslator.CANONICAL_URI, ImmutableSet.copyOf(uris)).build(), null, null, true);
//    }
//
//    List<Playlist> executePlaylistQuery(DBObject query, String type, Selection selection, boolean hydrate) {
//        List<Playlist> playlists = Lists.newArrayList();
//        try {
//
//            if (type != null) {
//                query.put("type", type);
//            }
//            Iterator<DBObject> cur = cursor(contentGroupCollection, query, selection);
//            if (cur == null) {
//                return Collections.emptyList();
//            }
//            int loaded = 0;
//            try {
//                while (cur.hasNext()) {
//                    DBObject current = cur.next();
//                    playlists.add(toPlaylist(current, hydrate));
//                    loaded++;
//                    if (loaded > MAX_RESULTS) {
//                        throw new IllegalArgumentException("Too many results for query");
//                    }
//                }
//            } catch (IllegalArgumentException e) {
//                LOG.error("IllegalArgumentThrown: " + e.getMessage() + ". Query was: " + query + ", and Selection: " + selection);
//                throw e;
//            }
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
//
//        return playlists;
//    }
//

	
	private final Function<DBObject, Identified> TO_MODEL = new Function<DBObject, Identified>() {

		@Override
		public Identified apply(DBObject dbo) {
			return toModel(dbo);
		}
	};
    
    @Override
    public Iterator<Content> listAllRoots() {
        throw new UnsupportedOperationException("TODO");
        //return Iterators.transform(contentGroupCollection.find(), TO_PLAYIST);
    }

	private Identified toModel(DBObject object) {
		Identified item = null;
		if (!object.containsField("type")) {
			throw new IllegalStateException("Missing type field");
		}
		String type = (String) object.get("type");
		if (Brand.class.getSimpleName().equals(type)) {
			item = fromDB(object, Brand.class);
		} else if (Series.class.getSimpleName().equals(type)) {
			item = fromDB(object, Series.class);
		} else if (Container.class.getSimpleName().equals(type)) {
			item = fromDB(object, Container.class);
		} else if (Episode.class.getSimpleName().equals(type)) {
			item = fromDB(object, Episode.class);
		} else if (Clip.class.getSimpleName().equals(type)) {
			item = fromDB(object, Clip.class);
		} else if (Item.class.getSimpleName().equals(type)) {
			item = fromDB(object, Item.class);
		} else {
			throw new IllegalArgumentException("Unknown type: " + type);
		}
		return item;
	}



//    private Playlist toPlaylist(DBObject object, boolean hydrate) {
//        Playlist playlist = null;
//
//        try {
//            if (object.containsField("type") && Brand.class.getSimpleName().equals(object.get("type"))) {
//                playlist = fromDB(object, Brand.class);
//            } else if (object.containsField("type") && Series.class.getSimpleName().equals(object.get("type"))) {
//                playlist = fromDB(object, Series.class);
//            } else {
//                playlist = fromDB(object, Playlist.class);
//            }
//
//            if (hydrate) {
//                List<Item> items = findItemsByCanonicalUri(playlist.getItemUris());
//                playlist.setItems(items);
//
//                List<Playlist> subPlaylists = findHydratedPlaylistsByCanonicalUri(playlist.getPlaylistUris());
//                playlist.setPlaylists(subPlaylists);
//            }
//            removeUriFromAliases(playlist);
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
//        return playlist;
//    }
}
