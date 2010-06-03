package org.uriplay.persistence.media.entity;

import org.uriplay.media.entity.Brand;
import org.uriplay.media.entity.Episode;

import com.metabroadcast.common.persistence.translator.ModelTranslator;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.DBObject;

public class EpisodeTranslator implements ModelTranslator<Episode> {
    private final ItemTranslator itemTranslator;
    private final BrandTranslator brandTranslator;
    
    public EpisodeTranslator(ItemTranslator itemTranslator, BrandTranslator brandTranslator) {
        this.itemTranslator = itemTranslator;
        this.brandTranslator = brandTranslator;
    }
    
    @Override
    public Episode fromDBObject(DBObject dbObject, Episode entity) {
        if (entity == null) {
            entity = new Episode();
        }
        
        itemTranslator.fromDBObject(dbObject, entity);
        
        entity.setEpisodeNumber((Integer) dbObject.get("episodeNumber"));
        entity.setSeriesNumber((Integer) dbObject.get("seriesNumber"));
        
        if (dbObject.containsField("brand")) {
            Brand brand = brandTranslator.fromDBObject((DBObject) dbObject.get("brand"), null);
            entity.setBrand(brand);
        }
        
        return entity;
    }

    @Override
    public DBObject toDBObject(DBObject dbObject, Episode entity) {
        dbObject = itemTranslator.toDBObject(dbObject, entity);
        
        TranslatorUtils.from(dbObject, "episodeNumber", entity.getEpisodeNumber());
        TranslatorUtils.from(dbObject, "seriesNumber", entity.getSeriesNumber());
        
        if (entity.getBrand() != null) {
            DBObject brand = brandTranslator.toDBObject(null, entity.getBrand());
            dbObject.put("brand", brand);
        }
        dbObject.put("type", Episode.class.getSimpleName());
        
        return dbObject;
    }

}
