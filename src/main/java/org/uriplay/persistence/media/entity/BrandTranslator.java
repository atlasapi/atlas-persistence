package org.uriplay.persistence.media.entity;

import org.uriplay.media.entity.Brand;

import com.metabroadcast.common.persistence.translator.ModelTranslator;
import com.mongodb.DBObject;

public class BrandTranslator implements ModelTranslator<Brand> {
    
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
        
        return entity;
    }

    @Override
    public DBObject toDBObject(DBObject dbObject, Brand entity) {
        dbObject = playlistTranslator.toDBObject(dbObject, entity);
        
        dbObject.put("type", Brand.class.getSimpleName());
        
        return dbObject;
    }

}
