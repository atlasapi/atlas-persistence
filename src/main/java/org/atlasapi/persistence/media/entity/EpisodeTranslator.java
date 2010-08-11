package org.atlasapi.persistence.media.entity;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Series;
import org.atlasapi.persistence.ModelTranslator;
import org.atlasapi.persistence.media.entity.SeriesTranslator.SeriesSummaryTranslator;

import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.DBObject;

public class EpisodeTranslator implements ModelTranslator<Episode> {
   
	private static final String EMBEDDED_SERIES_KEY = "series";
	
	private final ItemTranslator itemTranslator = new ItemTranslator();
    private final BrandTranslator brandTranslator = new BrandTranslator();
    private final SeriesSummaryTranslator seriesTranslator = new SeriesTranslator.SeriesSummaryTranslator();
    
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
        if (dbObject.containsField(EMBEDDED_SERIES_KEY)) {
        	 Series series = seriesTranslator.fromDBObject((DBObject) dbObject.get(EMBEDDED_SERIES_KEY));
             entity.setSeries(series);
        }
        return entity;
    }

    @Override
    public DBObject toDBObject(DBObject dbObject, Episode entity) {
        dbObject = itemTranslator.toDBObject(dbObject, entity);
        
        TranslatorUtils.from(dbObject, "episodeNumber", entity.getEpisodeNumber());
        TranslatorUtils.from(dbObject, "seriesNumber", entity.getSeriesNumber());
        
        if (entity.getBrand() != null) {
            DBObject brand = brandTranslator.toDBObjectForEmbeddedBrand(null, entity.getBrand());
            dbObject.put("brand", brand);
        }
        
        Series seriesSummary = entity.getSeriesSummary();
		if (seriesSummary != null) {
        	DBObject series = seriesTranslator.toDBObjectForSummary(seriesSummary);
            dbObject.put(EMBEDDED_SERIES_KEY, series);
        }
        
        dbObject.put("type", Episode.class.getSimpleName());
        
        return dbObject;
    }

}
