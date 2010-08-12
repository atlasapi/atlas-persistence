package org.atlasapi.persistence.media.entity;

import org.atlasapi.media.entity.Series;

import com.mongodb.DBObject;

public class SeriesTranslator {
    
	private static final String SERIES_NUMBER_KEY = "seriesNumber";
	
	private final PlaylistTranslator playlistTranslator ;

	public SeriesTranslator() {
		this(new PlaylistTranslator());
	}
	
	public SeriesTranslator(PlaylistTranslator playlistTranslator) {
		this.playlistTranslator = playlistTranslator;
	}
	
    public Series fromDBObject(DBObject dbObject) {
        Series entity = new Series();
        playlistTranslator.fromDBObject(dbObject, entity);
        entity.withSeriesNumber((Integer) dbObject.get(SERIES_NUMBER_KEY));
        return entity;
    }

    public DBObject toDBObject(Series entity) {
        DBObject dbObject = playlistTranslator.toDBObject(null, entity);
        if (entity.getSeriesNumber() != null) {
        	dbObject.put(SERIES_NUMBER_KEY, entity.getSeriesNumber());
        }
        dbObject.put("type", Series.class.getSimpleName());
        return dbObject;
    }

    
   static class SeriesSummaryTranslator {
    	
    	private final SeriesTranslator seriesTranslator;

		public SeriesSummaryTranslator() {
    		this.seriesTranslator = new SeriesTranslator(new PlaylistTranslator(new ContentTranslator(new DescriptionTranslator(false))));
    	}
		
		public DBObject toDBObjectForSummary(Series series) {
			return seriesTranslator.toDBObject(series);
		}

		public Series fromDBObject(DBObject dbObject) {
			Series series = seriesTranslator.fromDBObject(dbObject);
			series.markAsSummary();
			return series;
		}
    }
}
