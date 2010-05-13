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

package org.uriplay.persistence.content.mongodb;

import java.util.List;

import org.uriplay.media.entity.Brand;
import org.uriplay.media.entity.Episode;
import org.uriplay.media.entity.Item;
import org.uriplay.media.entity.Playlist;
import org.uriplay.persistence.media.entity.BrandTranslator;
import org.uriplay.persistence.media.entity.BroadcastTranslator;
import org.uriplay.persistence.media.entity.ContentMentionTranslator;
import org.uriplay.persistence.media.entity.DescriptionTranslator;
import org.uriplay.persistence.media.entity.EncodingTranslator;
import org.uriplay.persistence.media.entity.EpisodeTranslator;
import org.uriplay.persistence.media.entity.ItemTranslator;
import org.uriplay.persistence.media.entity.LocationTranslator;
import org.uriplay.persistence.media.entity.PlaylistTranslator;
import org.uriplay.persistence.media.entity.PolicyTranslator;
import org.uriplay.persistence.media.entity.VersionTranslator;
import org.uriplay.persistence.tracking.ContentMention;

import com.google.common.collect.Lists;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

public class MongoDBTemplate {
    private final DescriptionTranslator descriptionTranslator = new DescriptionTranslator();
    private final BroadcastTranslator broadcastTranslator = new BroadcastTranslator(descriptionTranslator);
    private final LocationTranslator locationTranslator = new LocationTranslator(descriptionTranslator, new PolicyTranslator());
    private final EncodingTranslator encodingTranslator = new EncodingTranslator(descriptionTranslator, locationTranslator);
    private final VersionTranslator versionTranslator = new VersionTranslator(descriptionTranslator, broadcastTranslator, encodingTranslator);
    private final ItemTranslator itemTranslator = new ItemTranslator(descriptionTranslator, versionTranslator);
    private final PlaylistTranslator playlistTranslator = new PlaylistTranslator(descriptionTranslator);
    private final BrandTranslator brandTranslator = new BrandTranslator(playlistTranslator);
    private final EpisodeTranslator episodeTranslator = new EpisodeTranslator(itemTranslator, brandTranslator);
    private final ContentMentionTranslator contentMentionTranslator = new ContentMentionTranslator();

    private final DB db;

    public MongoDBTemplate(Mongo mongo, String dbName) {
        db = mongo.getDB("uriplay");
    }

    protected final DBCollection table(String name) {
        return db.getCollection(name);
    }

    protected final BasicDBObject in(String attribute, List<?> elems) {
        return new BasicDBObject(attribute, new BasicDBObject("$in", list(elems)));
    }

    private BasicDBList list(List<?> elems) {
        BasicDBList dbList = new BasicDBList();
        dbList.addAll(elems);
        return dbList;
    }

    protected DBObject formatForDB(Object obj) {
        try {
            if (obj instanceof Brand) {
                return brandTranslator.toDBObject(null, (Brand) obj);
            } else if (obj instanceof Episode) {
                return episodeTranslator.toDBObject(null, (Episode) obj);
            } else if (obj instanceof Item) {
                return itemTranslator.toDBObject(null, (Item) obj);
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
    protected <T> T fromDb(DBObject dbObject, Class<T> clazz) {
        try {
            if (clazz.equals(Item.class)) {
                return (T) itemTranslator.fromDBObject(dbObject, null);
            } else if (clazz.equals(Playlist.class)) {
                return (T) playlistTranslator.fromDBObject(dbObject, null);
            } else if (clazz.equals(Brand.class)) {
                return (T) brandTranslator.fromDBObject(dbObject, null);
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

    protected <T> List<T> toList(DBCursor cursor, Class<T> type) {
        List<T> asList = Lists.newArrayList();
        while (cursor.hasNext()) {
            asList.add(fromDb(cursor.next(), type));
        }
        return asList;
    }
}
