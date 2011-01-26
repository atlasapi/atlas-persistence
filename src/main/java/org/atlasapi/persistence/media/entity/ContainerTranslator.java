package org.atlasapi.persistence.media.entity;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Series;
import org.atlasapi.persistence.ModelTranslator;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.mongodb.DBObject;

public class ContainerTranslator implements ModelTranslator<Container<?>> {
    
	private static final String CONTENTS_KEY = "contents";
	private static final String SERIES_NUMBER_KEY = "seriesNumber";
	
	private static final String FULL_SERIES_KEY = "series";

	private final ContentTranslator contentTranslator;
	
	private final Function<DBObject, Item> embeddedItemFromDbo;
	private final Function<Item, DBObject> embeddedItemToDbo;
	
    public ContainerTranslator() {
    	this.contentTranslator = new ContentTranslator();
    	final ItemTranslator embeddedItemTranslator = new ItemTranslator();
    	
    	this.embeddedItemFromDbo = new Function<DBObject, Item>() {
			@Override
			public Item apply(DBObject itemDbo) {
				return embeddedItemTranslator.fromDBObject(itemDbo, null);
			}
		};
		this.embeddedItemToDbo = new Function<Item, DBObject>() {
			@Override
			public DBObject apply(Item item) {
				return embeddedItemTranslator.toDBObject(null, item);
			}
		};
    }

    @Override
	@SuppressWarnings("unchecked")
    public Container<?> fromDBObject(DBObject dbObject, Container<?> entity) {
        if (entity == null) {
            entity = newModel(dbObject);
        }
        contentTranslator.fromDBObject(dbObject, entity);
        
        if (dbObject.containsField(CONTENTS_KEY)) {
        	Iterable<DBObject> contentDbos = (Iterable<DBObject>) dbObject.get(CONTENTS_KEY);
			((Container<Item>) entity).setContents(Iterables.transform(contentDbos, embeddedItemFromDbo));
        }

        if (entity instanceof Series) {
        	Series series = (Series) entity;
        	series.withSeriesNumber((Integer) dbObject.get(SERIES_NUMBER_KEY));
        }
        
        if (entity instanceof Brand) {
        	addSeriesToContents(dbObject, entity);
        }
        return entity;
    }

	@SuppressWarnings("unchecked")
	private void addSeriesToContents(DBObject dbObject, Container<?> entity) {
		Iterable<DBObject> seriesDbos = (Iterable<DBObject>) dbObject.get(FULL_SERIES_KEY);
		if (seriesDbos != null) {
			ImmutableMap<String,Series> lookup = seriesLookup(seriesDbos);
			for (Episode episode : ((Brand) entity).getContents()) {
				String seriesUri = episode.getSeriesUri();
				if (seriesUri != null) {
					Series series = lookup.get(seriesUri);
					if (series != null) {
						series.addContents(episode);
					}
				}
			}
		}
	}

	private ImmutableMap<String,Series> seriesLookup(Iterable<DBObject> seriesDbos) {
		ImmutableList<Series> series = ImmutableList.copyOf(Iterables.transform(seriesDbos, new Function<DBObject, Series>(){
			@Override
			public Series apply(DBObject seriesDbo) {
				return (Series) fromDBObject(seriesDbo, null);
			}}));
		return Maps.uniqueIndex(series, Identified.TO_URI);
	}

	private Container<?> newModel(DBObject dbo) {
		String type = (String) dbo.get("type");
		if (type.equals(Brand.class.getSimpleName())) {
			return new Brand();
		}
		if (type.equals(Series.class.getSimpleName())) {
			return new Series();
		}
		if (type.equals(Container.class.getSimpleName())) {
			return new Container<Item>();
		}
		throw new IllegalStateException();
	}

	@Override
    public DBObject toDBObject(DBObject dbObject, Container<?> entity) {
		dbObject = toDboNotIncludingContents(dbObject, entity);
        dbObject.put(CONTENTS_KEY, Iterables.transform(entity.getContents(), embeddedItemToDbo));
        if (entity instanceof Brand) {
        	Brand brand = (Brand) entity;
        	if (!brand.getSeries().isEmpty()) {
        		dbObject.put(FULL_SERIES_KEY, Iterables.transform(brand.getSeries(), new Function<Series, DBObject>() {
					@Override
					public DBObject apply(Series input) {
						return toDboNotIncludingContents(null, input);
					}
        		}));
        	}
        }
        return dbObject;
    }

	private DBObject toDboNotIncludingContents(DBObject dbObject, Container<?> entity) {
		dbObject = contentTranslator.toDBObject(dbObject, entity);
		dbObject.put("type", entity.getClass().getSimpleName());
        
        if (entity instanceof Series) {
        	Series series = (Series) entity;
	        if (series.getSeriesNumber() != null) {
	        	dbObject.put(SERIES_NUMBER_KEY, series.getSeriesNumber());
	        }
        }
		return dbObject;
	}
}
