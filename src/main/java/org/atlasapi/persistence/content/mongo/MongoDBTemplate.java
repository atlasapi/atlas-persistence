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

import static com.metabroadcast.common.persistence.mongo.MongoConstants.ID;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Clip;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.ContentGroup;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Series;
import org.atlasapi.persistence.media.entity.ContainerTranslator;
import org.atlasapi.persistence.media.entity.ItemTranslator;
import org.atlasapi.persistence.media.entity.ContentGroupTranslator;
import org.atlasapi.persistence.media.entity.SeriesTranslator;

import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.query.Selection;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class MongoDBTemplate {

	private final ItemTranslator itemTranslator = new ItemTranslator(true);
    private final ContentGroupTranslator playlistTranslator = new ContentGroupTranslator(true);
    private final ContainerTranslator brandTranslator = new ContainerTranslator(true);
    private final SeriesTranslator seriesTranslator = new SeriesTranslator(true);

    protected DBObject toDB(Object obj) {
        try {
            if (obj instanceof Container<?>) {
                return brandTranslator.toDBObject(null, (Container<?>) obj);
            } else if (obj instanceof Clip) {
            	throw new IllegalArgumentException("Direct-saving of clips is not supported");
            } else if (obj instanceof Item) {
                return itemTranslator.toDBObject(null, (Item) obj);
            } else if (obj instanceof Series) {
            	return seriesTranslator.toDBObject((Series) obj);
            } else if (obj instanceof ContentGroup) {
                return playlistTranslator.toDBObject(null, (ContentGroup) obj);
            } else {
                throw new IllegalArgumentException("Unabled to format "+obj+" for db, type: "+obj.getClass().getSimpleName());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected Identified toModel(DBObject dbo) {
		if (!dbo.containsField("type")) {
			throw new IllegalStateException("Missing type field");
		}
		String type = (String) dbo.get("type");
		if (Brand.class.getSimpleName().equals(type)) {
			return brandTranslator.fromDBObject(dbo, null);
		} 
		if (Series.class.getSimpleName().equals(type)) {
			return seriesTranslator.fromDBObject(dbo);
		} 
		if (Container.class.getSimpleName().equals(type)) {
			return brandTranslator.fromDBObject(dbo, null);
		} 
		if (Episode.class.getSimpleName().equals(type)) {
			return itemTranslator.fromDBObject(dbo, null);
		} 
		if (Clip.class.getSimpleName().equals(type)) {
			return itemTranslator.fromDBObject(dbo, null);
		}
		if (Item.class.getSimpleName().equals(type)) {
			return itemTranslator.fromDBObject(dbo, null);
		}
		if (ContentGroup.class.getSimpleName().equals(type)) {
			return playlistTranslator.fromDBObject(dbo, null);
		}
		throw new IllegalArgumentException("Unknown type: " + type);
	}
    
	private final static int DEFAULT_BATCH_SIZE = 50;
    
    private final DB db;

    public MongoDBTemplate(DatabasedMongo databasedMongo) {
        db = databasedMongo.database();
    }

    protected final DBCollection table(String name) {
        return db.getCollection(name);
    }
    
    protected final DBObject findById(DBCollection table, String id) {
		return table.findOne(new BasicDBObject(ID, id));
	}

    protected DBCursor cursor(DBCollection collection, DBObject query, Selection selection) {
        if (selection != null && (selection.hasLimit() || selection.hasNonZeroOffset())) {
            return collection.find(query, new BasicDBObject(), selection.getOffset(), hardLimit(selection));
        } else {
            return collection.find(query, new BasicDBObject(), 0, DEFAULT_BATCH_SIZE);
        }
    }

    private Integer hardLimit(Selection selection) {
        return -1 * selection.limitOrDefaultValue(0);
    }
}
