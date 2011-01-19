package org.atlasapi.persistence.media.entity;

import java.util.List;
import java.util.Set;

import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Series;
import org.atlasapi.media.entity.Version;
import org.atlasapi.persistence.ModelTranslator;
import org.atlasapi.persistence.media.entity.SeriesTranslator.SeriesSummaryTranslator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBList;
import com.mongodb.DBObject;

public class ItemTranslator implements ModelTranslator<Item> {
    
	private static final String VERSIONS_KEY = "versions";
	private static final String TYPE_KEY = "type";
	private static final String IS_LONG_FORM_KEY = "isLongForm";
	private static final String EMBEDDED_SERIES_KEY = "series";
	private static final String SYNTHETIC_KEY = "synthetic";
	
	private final ContentTranslator contentTranslator;
    private final VersionTranslator versionTranslator = new VersionTranslator();
    
    private final SeriesSummaryTranslator embeddedSeriesTranslator = new SeriesTranslator.SeriesSummaryTranslator();

    ItemTranslator(ContentTranslator contentTranslator) {
		this.contentTranslator = contentTranslator;
    }
    
    public ItemTranslator(boolean useIds) {
    	this(new ContentTranslator(useIds));
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public Item fromDBObject(DBObject dbObject, Item item) {
        if (Boolean.TRUE.equals(dbObject.get(SYNTHETIC_KEY))) {
        	dbObject = Iterables.getOnlyElement((Iterable<DBObject>) dbObject.get("contents"));
        }
    	if (item == null) {
        	item = newModel(dbObject, item);
        }
        
        contentTranslator.fromDBObject(dbObject, item);
        
        item.setIsLongForm((Boolean) dbObject.get(IS_LONG_FORM_KEY));
        
        List<DBObject> list = (List) dbObject.get(VERSIONS_KEY);
        if (list != null && ! list.isEmpty()) {
            Set<Version> versions = Sets.newHashSet();
            for (DBObject object: list) {
                Version version = versionTranslator.fromDBObject(object, null);
                versions.add(version);
            }
            item.setVersions(versions);
        }
        
//        if (dbObject.containsField(EMBEDDED_CONTAINER_KEY)) {
//            Container<?> brand = embeddedContainerTranslator.fromDBObject((DBObject) dbObject.get(EMBEDDED_CONTAINER_KEY), null);
//            entity.setContainer(brand);
//        }

        if (item instanceof Episode) {
        	Episode episode = (Episode) item;
        	if (dbObject.containsField(EMBEDDED_SERIES_KEY)) {
        		Series series = embeddedSeriesTranslator.fromDBObject((DBObject) dbObject.get(EMBEDDED_SERIES_KEY));
        		episode.setSeries(series);
        	}

        	episode.setEpisodeNumber((Integer) dbObject.get("episodeNumber"));
        	episode.setSeriesNumber((Integer) dbObject.get("seriesNumber"));
        }
        
        
        
        return item;
    }

	private Item newModel(DBObject dbObject, Item entity) {
		String type = (String) dbObject.get(TYPE_KEY);
		if (Episode.class.getSimpleName().equals(type)) {
			entity = new Episode();
		} else if (Item.class.getSimpleName().equals(type)) {
			entity = new Item();
		} else {
			throw new IllegalArgumentException();
		}
		return entity;
	}

    @Override
    public DBObject toDBObject(DBObject itemDbo, Item entity) {
        itemDbo = contentTranslator.toDBObject(itemDbo, entity);
        itemDbo.put(TYPE_KEY, entity.getClass().getSimpleName());

        itemDbo.put(IS_LONG_FORM_KEY, entity.getIsLongForm());
        if (! entity.getVersions().isEmpty()) {
            BasicDBList list = new BasicDBList();
            for (Version version: entity.getVersions()) {
                list.add(versionTranslator.toDBObject(null, version));
            }
            itemDbo.put(VERSIONS_KEY, list);
        }
        
//        if (entity.getBrand() != null) {
//            DBObject brand = embeddedContainerTranslator.toDBObject(null, entity.getBrand());
//            dbObject.put(EMBEDDED_CONTAINER_KEY, brand);
//        }
//        
		
		if (entity instanceof Episode) {
			Episode episode = (Episode) entity;
			TranslatorUtils.from(itemDbo, "episodeNumber", episode.getEpisodeNumber());
			TranslatorUtils.from(itemDbo, "seriesNumber", episode.getSeriesNumber());

			Series seriesSummary = episode.getSeriesSummary();
			if (seriesSummary != null) {
				DBObject series = embeddedSeriesTranslator.toDBObjectForSummary(seriesSummary);
				itemDbo.put(EMBEDDED_SERIES_KEY, series);
			}
		}
		
		if (entity.getContainer() == null) {
			DBObject containerDBO = contentTranslator.toDBObject(null, entity);
			containerDBO.put("contents", ImmutableList.of(itemDbo));
			containerDBO.put(TYPE_KEY, entity.getClass().getSimpleName());
			containerDBO.put(SYNTHETIC_KEY, true);
			return containerDBO;
		}
        return itemDbo;
    }
}
