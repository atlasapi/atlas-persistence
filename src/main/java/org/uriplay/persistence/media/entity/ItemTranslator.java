package org.uriplay.persistence.media.entity;

import java.util.List;
import java.util.Set;

import org.uriplay.media.entity.Item;
import org.uriplay.media.entity.Version;

import com.google.common.collect.Sets;
import com.mongodb.BasicDBList;
import com.mongodb.DBObject;

public class ItemTranslator implements DBObjectEntityTranslator<Item> {
    private final DescriptionTranslator descriptionTranslator;
    private final VersionTranslator versionTranslator;
    
    public ItemTranslator(DescriptionTranslator descriptionTranslator, VersionTranslator versionTranslator) {
        this.descriptionTranslator = descriptionTranslator;
        this.versionTranslator = versionTranslator;
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public Item fromDBObject(DBObject dbObject, Item entity) {
        if (entity == null) {
            entity = new Item();
        }
        
        descriptionTranslator.fromDBObject(dbObject, entity);
        
        entity.setContainedInUris(TranslatorUtils.toSet(dbObject, "containedInUris"));
        entity.setDescription((String) dbObject.get("description"));
        entity.setFirstSeen(TranslatorUtils.toDateTime(dbObject, "firstSeen"));
        entity.setGenres(TranslatorUtils.toSet(dbObject, "genres"));
        entity.setImage((String) dbObject.get("image"));
        entity.setIsLongForm((Boolean) dbObject.get("isLongForm"));
        entity.setLastFetched(TranslatorUtils.toDateTime(dbObject, "lastFetched"));
        entity.setPublisher((String) dbObject.get("publisher"));
        entity.setTags(TranslatorUtils.toSet(dbObject, "tags"));
        entity.setThumbnail((String) dbObject.get("thumbnail"));
        entity.setTitle((String) dbObject.get("title"));
        
        List<DBObject> list = (List) dbObject.get("versions");
        if (list != null && ! list.isEmpty()) {
            Set<Version> versions = Sets.newHashSet();
            for (DBObject object: list) {
                Version version = versionTranslator.fromDBObject(object, null);
                versions.add(version);
            }
            entity.setVersions(versions);
        }
        
        return entity;
    }

    @Override
    public DBObject toDBObject(DBObject dbObject, Item entity) {
        dbObject = descriptionTranslator.toDBObject(dbObject, entity);
        
        TranslatorUtils.fromSet(dbObject, entity.getContainedInUris(), "containedInUris");
        TranslatorUtils.from(dbObject, "description", entity.getDescription());
        TranslatorUtils.fromDateTime(dbObject, "firstSeen", entity.getFirstSeen());
        TranslatorUtils.fromSet(dbObject, entity.getGenres(), "genres");
        TranslatorUtils.from(dbObject, "image", entity.getImage());
        dbObject.put("isLongForm", entity.getIsLongForm());
        TranslatorUtils.fromDateTime(dbObject, "lastFetched", entity.getLastFetched());
        TranslatorUtils.from(dbObject, "publisher", entity.getPublisher());
        TranslatorUtils.fromSet(dbObject, entity.getTags(), "tags");
        TranslatorUtils.from(dbObject, "thumbnail", entity.getThumbnail());
        TranslatorUtils.from(dbObject, "title", entity.getTitle());
        
        if (! entity.getVersions().isEmpty()) {
            BasicDBList list = new BasicDBList();
            for (Version version: entity.getVersions()) {
                list.add(versionTranslator.toDBObject(null, version));
            }
            dbObject.put("versions", list);
        }
        
        return dbObject;
    }
}
