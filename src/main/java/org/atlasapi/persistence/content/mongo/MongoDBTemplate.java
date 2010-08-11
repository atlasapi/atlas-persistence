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
import static com.metabroadcast.common.persistence.mongo.MongoConstants.IN;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Playlist;
import org.atlasapi.media.entity.Series;
import org.atlasapi.persistence.media.entity.BrandTranslator;
import org.atlasapi.persistence.media.entity.ContentMentionTranslator;
import org.atlasapi.persistence.media.entity.EpisodeTranslator;
import org.atlasapi.persistence.media.entity.ItemTranslator;
import org.atlasapi.persistence.media.entity.PlaylistTranslator;
import org.atlasapi.persistence.media.entity.SeriesTranslator;
import org.atlasapi.persistence.tracking.ContentMention;

import com.google.common.collect.Lists;
import com.metabroadcast.common.query.Selection;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

public class MongoDBTemplate {

    private final ItemTranslator itemTranslator = new ItemTranslator();
    private final PlaylistTranslator playlistTranslator = new PlaylistTranslator();
    private final BrandTranslator brandTranslator = new BrandTranslator();
    private final EpisodeTranslator episodeTranslator = new EpisodeTranslator();
    private final ContentMentionTranslator contentMentionTranslator = new ContentMentionTranslator();
    private final SeriesTranslator seriesTranslator = new SeriesTranslator();

    protected DBObject toDB(Object obj) {
        try {
            if (obj instanceof Brand) {
                return brandTranslator.toDBObject(null, (Brand) obj);
            } else if (obj instanceof Episode) {
                return episodeTranslator.toDBObject(null, (Episode) obj);
            } else if (obj instanceof Item) {
                return itemTranslator.toDBObject(null, (Item) obj);
            } else if (obj instanceof Series) {
            	return seriesTranslator.toDBObject((Series) obj);
            } else if (obj instanceof Playlist) {
                return playlistTranslator.toDBObject(null, (Playlist) obj);
            } else if (obj instanceof ContentMention) {
                return contentMentionTranslator.toDBObject(null, (ContentMention) obj);
            } else {
                throw new IllegalArgumentException("Unabled to format "+obj+" for db, type: "+obj.getClass().getSimpleName());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    protected <T> T fromDB(DBObject dbObject, Class<T> clazz) {
        try {
            if (clazz.equals(Item.class)) {
                return (T) itemTranslator.fromDBObject(dbObject, null);
            } else if (clazz.equals(Playlist.class)) {
                return (T) playlistTranslator.fromDBObject(dbObject, null);
            } else if (clazz.equals(Brand.class)) {
                return (T) brandTranslator.fromDBObject(dbObject, null);
            } else if (clazz.equals(Series.class)) {
            	return (T) seriesTranslator.fromDBObject(dbObject);
            } else if (clazz.equals(Episode.class)) {
                return (T) episodeTranslator.fromDBObject(dbObject, null);
            } else if (clazz.equals(ContentMention.class)) {
                return (T) contentMentionTranslator.fromDBObject(dbObject, null);
            } else {
                throw new IllegalArgumentException("Unabled to read "+dbObject+" from db, type: "+clazz.getSimpleName());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
	private final static int DEFAULT_BATCH_SIZE = 50;
    
    private final DB db;

    public MongoDBTemplate(Mongo mongo, String dbName) {
        db = mongo.getDB(dbName);
    }

    protected final DBCollection table(String name) {
        return db.getCollection(name);
    }
    
    protected final DBObject findById(DBCollection table, String id) {
		return table.findOne(new BasicDBObject(ID, id));
	}

    protected final BasicDBObject in(String attribute, Set<?> elems) {
        return new BasicDBObject(attribute, new BasicDBObject(IN, list(elems)));
    }

    private BasicDBList list(Set<?> elems) {
        BasicDBList dbList = new BasicDBList();
        dbList.addAll(elems);
        return dbList;
    }

    protected <T> List<T> toList(DBCursor cursor, Class<T> type) {
        List<T> asList = Lists.newArrayList();
        while (cursor.hasNext()) {
            asList.add(fromDB(cursor.next(), type));
        }
        return asList;
    }

    
    protected <T> List<T> executeQuery(DBCollection collection, Class<T> clazz, DBObject query, Selection selection) {
        return elementsFrom(clazz, cursor(collection, query, selection));
    }
    
    protected <T> List<T> executeQuery(DBCollection collection, Class<T> clazz, DBObject query) {
        return elementsFrom(clazz,  collection.find(query, new BasicDBObject()));
    }
    
    protected <T> List<T> executeQuery(DBCollection collection, Class<T> clazz, DBObject query, DBObject sort) {
        return elementsFrom(clazz, collection.find(query, new BasicDBObject()).sort(sort));
    }

	private <T> List<T> elementsFrom(Class<T> clazz, Iterator<DBObject> cur) {
		if (cur == null) {
            return Collections.emptyList();
        }
        List<T> elements = Lists.newArrayList();
        while (cur.hasNext()) {
            DBObject current = cur.next();
            elements.add(fromDB(current, clazz));
        }
        return elements;
	}

    protected Iterator<DBObject> cursor(DBCollection collection, DBObject query, Selection selection) {
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
