package org.atlasapi.persistence.media.entity;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.persistence.ModelTranslator;

import com.mongodb.DBObject;

public class BrandTranslator implements ModelTranslator<Brand> {
    
	private final PlaylistTranslator playlistTranslator = new PlaylistTranslator();

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

	public DBObject toDBObjectForEmbeddedBrand(DBObject dbObject, Brand brand) {
       return new PlaylistTranslator(new ContentTranslator(new DescriptionTranslator(false), new ClipTranslator())).toDBObject(dbObject, brand);
	}
}
