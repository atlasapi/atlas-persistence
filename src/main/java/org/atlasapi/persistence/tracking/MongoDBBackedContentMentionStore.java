/* Copyright 2010 Meta Broadcast Ltd

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License. You may
obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License. */

package org.atlasapi.persistence.tracking;

import java.util.List;

import org.atlasapi.persistence.content.mongo.MongoDBTemplate;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.Mongo;

public class MongoDBBackedContentMentionStore extends MongoDBTemplate implements PossibleContentUriMentionListener, ContentMentionStore {

	private final DBCollection contentMentions;

	public MongoDBBackedContentMentionStore(Mongo mongo, String dbName) {
		super(mongo, dbName);
		this.contentMentions = table("contentMentions");
	}
	
	@Override
	public void mentioned(ContentMention mention) {
		contentMentions.save(toDB(mention));
	}

	@Override
	public List<ContentMention> mentions(TrackingSource source, int limit) {
		DBCursor cursor = contentMentions.find().sort(new BasicDBObject("mentionedAt", "-1")).limit(limit);
		return toList(cursor, ContentMention.class);
	}
}
