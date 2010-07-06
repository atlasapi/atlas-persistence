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

package org.uriplay.persistence.content.mongo;

import java.util.List;

import org.uriplay.content.criteria.ContentQuery;
import org.uriplay.media.entity.Brand;
import org.uriplay.media.entity.Item;
import org.uriplay.media.entity.Playlist;

public class MongoRoughSearch {

    private final MongoDbBackedContentStore store;

	public MongoRoughSearch(MongoDbBackedContentStore store) {
		this.store = store;
    }

    private MongoDBQueryBuilder queryBuilder = new MongoDBQueryBuilder();

    public List<Item> itemsMatching(ContentQuery query) {
        return store.executeItemQuery(queryBuilder.buildItemQuery(query), query.getSelection());
    }
    
	@SuppressWarnings("unchecked")
	public List<Brand> dehydratedBrandsMatching(ContentQuery query) {
		return (List) store.executePlaylistQuery(queryBuilder.buildBrandQuery(query), Brand.class.getSimpleName(), query.getSelection(), false);
	}    
	
	@SuppressWarnings("unchecked")
	public List<Playlist> dehydratedPlaylistsMatching(ContentQuery query) {
		return (List) store.executePlaylistQuery(queryBuilder.buildPlaylistQuery(query), null, query.getSelection(), false);
	}    
}
