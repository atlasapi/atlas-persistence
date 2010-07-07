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

package org.atlasapi.persistence.content.mongo;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import junit.framework.TestCase;

import org.atlasapi.persistence.tracking.ContentMention;
import org.atlasapi.persistence.tracking.MongoDBBackedContentMentionStore;
import org.atlasapi.persistence.tracking.TrackingSource;
import org.joda.time.DateTime;

import com.google.common.collect.Iterables;
import com.metabroadcast.common.persistence.MongoTestHelper;
import com.mongodb.Mongo;


public class MongoDbBackedContentMentionStoreTest extends TestCase {
	
    private static final String DB_NAME = "uriplay";
    
	private MongoDBBackedContentMentionStore store;
    
    @Override
    protected void setUp() throws Exception {
    	super.setUp();
    	Mongo mongo = MongoTestHelper.anEmptyMongo();
    	store = new MongoDBBackedContentMentionStore(mongo, DB_NAME);
    	mongo.getDB(DB_NAME).eval("db.contentMentions.ensureIndex({uri:1, externalRef:1, source: 1}, {unique : true, dropDups : true})");
    	mongo.getDB(DB_NAME).eval("db.contentMentions.ensureIndex({mentionedAt:-1})");
    }

	public void testSavingAMention() throws Exception {
		ContentMention mention = new ContentMention("/uri", TrackingSource.TWITTER, "101", new DateTime());
		
		// should only be saved once
		store.mentioned(mention);
		store.mentioned(mention);

		ContentMention fromDatabase = Iterables.getOnlyElement(store.mentions(TrackingSource.TWITTER, 100));
		assertThat(fromDatabase, is(mention));
	}
}
