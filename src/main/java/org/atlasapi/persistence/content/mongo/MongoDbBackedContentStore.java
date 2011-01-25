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
import static com.metabroadcast.common.persistence.mongo.MongoBuilders.where;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.atlasapi.content.criteria.ContentQuery;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Broadcast;
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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.query.Selection;
import com.metabroadcast.common.time.Clock;
import com.metabroadcast.common.time.DateTimeZones;
import com.metabroadcast.common.time.SystemClock;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class MongoDbBackedContentStore extends MongoDBTemplate implements ContentWriter, ContentResolver, RetrospectiveContentLister, AliasWriter {

    private final static int MAX_RESULTS = 20000;

    private final Log log = LogFactory.getLog(getClass());

    private final Clock clock;
    private final DBCollection contentCollection;
    private final DBCollection contentGroupCollection;

    public MongoDbBackedContentStore(DatabasedMongo mongo, Clock clock) {
        super(mongo);
		this.clock = clock;
        contentCollection = table("content");
        contentGroupCollection = table("groups");
    }

    public MongoDbBackedContentStore(DatabasedMongo mongo) {
    	this(mongo, new SystemClock());
	}

	@Override
    public void addAliases(String uri, Set<String> aliases) {
		// TODO, this is a very crass way to edit one piece of data
		Identified identified = findByCanonicalUri(uri);
		if (identified == null) {
			return;
		}
		identified.addAliases(aliases);
		if (identified instanceof Item) {
			createOrUpdate((Item) identified);
		} 
		if (identified instanceof Container<?>) {
			createOrUpdate((Container<?>) identified, false);
		}
	}

    @Override
    public void createOrUpdate(Item item) {
        createOrUpdateItem(item);
    }

	@SuppressWarnings("unchecked")
	private  void addOrReplace(Item item, Container<?> container) {
		if (!container.getContents().contains(item)) {
			((Container<Item>) container).addContents(item);
		} else { // replace
			List<Item> currentItems = Lists.newArrayList(container.getContents());
			currentItems.set(currentItems.indexOf(item), item);
			((Container<Item>) container).setContents(currentItems);
		}
	}

	private void createOrUpdateItem(Item item) {
		updateFetchData(item);
		Identified content = findByCanonicalUri(item.getCanonicalUri());
		if (content == null) {
			item.setFirstSeen(clock.now());

			if (item.getContainer() != null) {
				Container<?> container = (Container<?>) findByCanonicalUri(item.getContainer().getCanonicalUri());
				if (container != null) {
					addOrReplace(item, container);
					item.setContainer(container);
				}
			}
		} else {
			if (!(content instanceof Item)) {
				throw new IllegalArgumentException("Cannot update item with uri: " + item.getCanonicalUri() + "  since the old entity was not an item");
			}
			Item oldItem = (Item) content;
			preserveAliases(item, oldItem);

			if (oldItem.getFullContainer() != null) {
				addOrReplace(item, oldItem.getFullContainer());
				item.setContainer(oldItem.getFullContainer());
			}
		}

		Container<?> container = item.getFullContainer();
		if (container != null) {
			updateBasicPlaylistDetails(container, contentCollection);
		} else {
			DBObject query = new BasicDBObject();
			query.put(DescriptionTranslator.CANONICAL_URI, item.getCanonicalUri());
			contentCollection.update(query, toDB(item), true, false);
		}
	}

	private void updateFetchData(Item item) {
		item.setLastFetched(new DateTime());
		setThisOrChildLastUpdated(item);
	}
    
    @Override
    @SuppressWarnings("unchecked")
    public void createOrUpdate(Container<?> container, boolean markMissingItemsAsUnavailable) {
		for (Item item : container.getContents()) {
			updateFetchData(item);
		}
	
		Identified oldContent = findByCanonicalUri(container.getCanonicalUri());

		if (oldContent != null) {
			if (!(oldContent instanceof Container<?>)) {
				throw new IllegalStateException("Cannot save container " + container.getCanonicalUri() + " because there's already an item with that uri");
			}
			Container<? extends Item> oldContainer = (Container<?>) oldContent;
        	
        	Set<Item> missingItems = Sets.difference(ImmutableSet.copyOf(oldContainer.getContents()), ImmutableSet.copyOf(container.getContents()));

			for (Item item : missingItems) {
				if (markMissingItemsAsUnavailable) {
					markAllNativeVersionsAsUnavailable(item);
				}
				((Container<Item>) container).addContents(item);
			}
			preservePlaylistAttributes(container, oldContainer);
        }

        if (oldContent == null) {
            container.setFirstSeen(new DateTime());
        }
        
        container.setLastFetched(new DateTime());
        setThisOrChildLastUpdated(container);

        updateBasicPlaylistDetails(container, contentCollection);
        
        if (container instanceof Brand) {
        	Brand brand = (Brand) container;
        	Set<Series> series = fullSeriesFrom(brand);
        	for (Series sery : series) {
        		createOrUpdateSkeleton(sery);
			}
        }
    }

	private void markAllNativeVersionsAsUnavailable(Item item) {
		for (Version version : item.nativeVersions()) {
		    for (Encoding encoding : version.getManifestedAs()) {
		        for (Location location : encoding.getAvailableAt()) {
		            location.setAvailable(false);
		            location.setLastUpdated(new DateTime(DateTimeZones.UTC));
		        }
		    }
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
		return findByUriOrAlias(uris, true);
	}

	private List<? extends Identified> findByUriOrAlias(Iterable<String> uris, boolean includeGroups) {
		final ImmutableSet<String> uriSet = ImmutableSet.copyOf(uris);
		
       	Function<Identified, Identified> extractItem = new Function<Identified, Identified>() {

			@Override
			public Identified apply(Identified content) {
				return extactItemIfInternalUrl(content, uriSet);
			}
       		
       	};
		Iterable<Identified> foundContent = Iterables.transform(where().fieldIn(DescriptionTranslator.LOOKUP, uris).find(contentCollection), Functions.compose(extractItem, TO_MODEL));

		if (includeGroups) {
		// TODO, only do this if missing uris
			Iterable<Identified> foundGroups = findGroupsByCanonicalUri(uris, extractItem);
			return ImmutableList.copyOf(Iterables.concat(foundContent, foundGroups));
		}
		return ImmutableList.copyOf(foundContent);
	}

	private Iterable<Identified> findGroupsByCanonicalUri(Iterable<String> uris, Function<Identified, Identified> extractItem) {
		Iterable<Identified> groups = findDehydratedGroupsByCanonicalUri(uris, extractItem);
		Set<String> itemUris = Sets.newHashSet();
		for (Identified identified : groups) {
			ContentGroup group = (ContentGroup) identified;
			itemUris.addAll(group.getContentUris());
		}
		ImmutableMap<String, Content> lookup = Maps.uniqueIndex(findContentByCanonicalUri(itemUris), Identified.TO_URI);
		for (Identified identified : groups) {
			ContentGroup group = (ContentGroup) identified;
			
			List<Content> content = Lists.newArrayList();
			for (String itemUri : group.getContentUris()) {
				content.add(lookup.get(itemUri));
			}
			group.setContentUris(ImmutableList.<String>of());
			group.setContents(content);
		}
		return groups;
	}

	private ImmutableList<Identified> findDehydratedGroupsByCanonicalUri(Iterable<String> uris, Function<Identified, Identified> extractItem) {
		return ImmutableList.copyOf(Iterables.transform(where().fieldIn(DescriptionTranslator.CANONICAL_URI, uris).find(contentGroupCollection), Functions.compose(extractItem, TO_MODEL)));
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
		return ImmutableList.copyOf(extractCanonical(ImmutableSet.copyOf(uris), findByUriOrAlias(uris, true)));
    }

	@SuppressWarnings("unchecked")
	public List<Content> findContentByCanonicalUri(final Iterable<String> uris) {
		return (List) ImmutableList.copyOf(extractCanonical(ImmutableSet.copyOf(uris), findByUriOrAlias(uris, false)));
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

    List<Content> topLevelElements(DBObject query, Selection selection) {

        DBCursor cur = cursor(contentCollection, query, selection);

        if (cur == null) {
            return ImmutableList.of();
        }
        int loaded = 0;
        List<Content> items = Lists.newArrayList();
        
        while (cur.hasNext()) {
            DBObject current = cur.next();
            items.add((Content) toModel(current));
            loaded++;
            if (loaded > MAX_RESULTS) {
                throw new IllegalArgumentException("Too many results for query");
            }
        }
        return items;
    }

	
	private final Function<DBObject, Identified> TO_MODEL = new Function<DBObject, Identified>() {

		@Override
		public Identified apply(DBObject dbo) {
			return toModel(dbo);
		}
	};
    
	@Override
	@SuppressWarnings("unchecked")
    public Iterator<Content> listAllRoots() {
        return (Iterator) Iterators.transform(contentCollection.find(), TO_MODEL);
    }

	private final MongoDBQueryBuilder queryBuilder = new MongoDBQueryBuilder();

    public List<? extends Content> discover(ContentQuery query) {
        return topLevelElements(queryBuilder.buildQuery(query), query.getSelection());
    }
}
