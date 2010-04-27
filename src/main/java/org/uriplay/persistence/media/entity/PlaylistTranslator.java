package org.uriplay.persistence.media.entity;

import org.uriplay.media.entity.Playlist;

import com.mongodb.DBObject;

public class PlaylistTranslator implements DBObjectEntityTranslator<Playlist> {
    private final DescriptionTranslator descriptionTranslator;
    
    public PlaylistTranslator(DescriptionTranslator descriptionTranslator) {
        this.descriptionTranslator = descriptionTranslator;
    }

    @Override
    public Playlist fromDBObject(DBObject dbObject, Playlist entity) {
        if (entity == null) {
            entity = new Playlist();
        }
        
        descriptionTranslator.fromDBObject(dbObject, entity);
        
        entity.setContainedInUris(TranslatorUtils.toSet(dbObject, "containedInUris"));
        entity.setDescription((String) dbObject.get("description"));
        entity.setFirstSeen(TranslatorUtils.toDateTime(dbObject, "firstSeen"));
        entity.setLastFetched(TranslatorUtils.toDateTime(dbObject, "lastFetched"));
        entity.setTitle((String) dbObject.get("title"));
        entity.setPublisher((String) dbObject.get("publisher"));
        entity.setPlaylistUris(TranslatorUtils.toList(dbObject, "playlistUris"));
        entity.setItemUris(TranslatorUtils.toList(dbObject, "itemUris"));
        
        return entity;
    }

    @Override
    public DBObject toDBObject(DBObject dbObject, Playlist entity) {
        dbObject = descriptionTranslator.toDBObject(dbObject, entity);
        
        TranslatorUtils.fromSet(dbObject, entity.getContainedInUris(), "containedInUris");
        TranslatorUtils.from(dbObject, "description", entity.getDescription());
        TranslatorUtils.fromDateTime(dbObject, "firstSeen", entity.getFirstSeen());
        TranslatorUtils.from(dbObject, "title", entity.getTitle());
        TranslatorUtils.from(dbObject, "publisher", entity.getPublisher());
        TranslatorUtils.fromList(dbObject, entity.getItemUris(), "itemUris");
        TranslatorUtils.fromList(dbObject, entity.getPlaylistUris(), "playlistUris");
        TranslatorUtils.fromDateTime(dbObject, "lastFetched", entity.getLastFetched());
        
        return dbObject;
    }

}
