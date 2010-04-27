package org.uriplay.persistence.media.entity;

import org.uriplay.media.entity.Brand;

import com.mongodb.DBObject;

public class BrandTranslator implements DBObjectEntityTranslator<Brand> {
    private final PlaylistTranslator playlistTranslator;
    
    public BrandTranslator(PlaylistTranslator playlistTranslator) {
        this.playlistTranslator = playlistTranslator;
    }

    @Override
    public Brand fromDBObject(DBObject dbObject, Brand entity) {
        if (entity == null) {
            entity = new Brand();
        }
        
        playlistTranslator.fromDBObject(dbObject, entity);
        
        entity.setGenres(TranslatorUtils.toSet(dbObject, "genre"));
        
        return entity;
    }

    @Override
    public DBObject toDBObject(DBObject dbObject, Brand entity) {
        dbObject = playlistTranslator.toDBObject(dbObject, entity);
        
        TranslatorUtils.fromSet(dbObject, entity.getGenres(), "genre");
        dbObject.put("type", Brand.class.getSimpleName());
        
        return dbObject;
    }

}
