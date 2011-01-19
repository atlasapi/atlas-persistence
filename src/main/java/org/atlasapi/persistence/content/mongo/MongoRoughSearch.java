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

import java.util.List;

import org.atlasapi.content.criteria.ContentQuery;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Identified;

public class MongoRoughSearch {

	private final MongoDBQueryBuilder queryBuilder = new MongoDBQueryBuilder();
	private final MongoDbBackedContentStore store;

	public MongoRoughSearch(MongoDbBackedContentStore store) {
		this.store = store;
    }

    public List<? extends Content> discover(ContentQuery query) {
        return store.executeDiscoverQuery(queryBuilder.buildQuery(query), query.getSelection());
    }
    
    public List<? extends Identified> findByUriOrAlias(Iterable<String> uris) {
        return store.findByUriOrAlias(uris);
    }
}
