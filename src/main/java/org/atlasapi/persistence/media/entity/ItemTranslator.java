package org.atlasapi.persistence.media.entity;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.intl.Countries;
import com.metabroadcast.common.intl.Country;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.atlasapi.media.entity.EntityType;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Film;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.media.entity.ReleaseDate;
import org.atlasapi.media.entity.ReleaseDate.ReleaseType;
import org.atlasapi.media.entity.Song;
import org.atlasapi.media.entity.Subtitles;
import org.atlasapi.persistence.ModelTranslator;
import org.atlasapi.persistence.content.mongo.DbObjectHashCodeDebugger;
import org.joda.time.Duration;
import org.joda.time.LocalDate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

public class ItemTranslator implements ModelTranslator<Item> {

    private static final Logger log = LoggerFactory.getLogger(ItemTranslator.class);
    
    public static final String CONTAINER = "container";
    public static final String SERIES = "series";
    public static final String SERIES_ID = "seriesId";
    public static final String CONTAINER_ID = "containerId";
    private static final String FILM_RELEASES_KEY = "releases";
    private static final String FILM_SUBTITLES_KEY = "subtitles";
    private static final String PART_NUMBER = "partNumber";
    private static final String EPISODE_NUMBER = "episodeNumber";
    private static final String SERIES_NUMBER = "seriesNumber";
    private static final String SPECIAL = "special";

	private static final String IS_LONG_FORM_KEY = "isLongForm";
	private static final String EPISODE_SERIES_URI_KEY = "seriesUri";
	private static final String FILM_WEBSITE_URL_KEY = "websiteUrl";
	private static final String BLACK_AND_WHITE_KEY = "blackAndWhite";
	private static final String DURATION_KEY = "duration";
    private static final String ISRC_KEY = "isrc";

    public static final Set<String> DB_KEYS = ImmutableSet.of(
            CONTAINER,
            SERIES,
            SERIES_ID,
            CONTAINER_ID,
            FILM_RELEASES_KEY,
            FILM_SUBTITLES_KEY,
            PART_NUMBER,
            EPISODE_NUMBER,
            SERIES_NUMBER,
            SPECIAL,
            IS_LONG_FORM_KEY,
            EPISODE_SERIES_URI_KEY,
            FILM_WEBSITE_URL_KEY,
            BLACK_AND_WHITE_KEY,
            DURATION_KEY,
            ISRC_KEY
    );

    private final ContentTranslator contentTranslator;

    private final DbObjectHashCodeDebugger dboHashCodeDebugger = new DbObjectHashCodeDebugger();

    private final Function<DBObject, Subtitles> subtitlesFromDbo = new Function<DBObject, Subtitles>() {
        @Override
        public Subtitles apply(DBObject input) {
            return new Subtitles(TranslatorUtils.toString(input, "language"));
        }
    };
    private final Function<DBObject, ReleaseDate> releaseDateFromDbo = new Function<DBObject, ReleaseDate>() {
        @Override
        public ReleaseDate apply(DBObject input) {
            LocalDate date = TranslatorUtils.toLocalDate(input, "date");
            Country country = Countries.fromCode(TranslatorUtils.toString(input, "country"));
            ReleaseType type = ReleaseType.valueOf(TranslatorUtils.toString(input, "type"));
            return new ReleaseDate(date, country, type);
        }
    };

    ItemTranslator(ContentTranslator contentTranslator, NumberToShortStringCodec idCodec) {
		this.contentTranslator = contentTranslator;
    }
    
    public ItemTranslator(NumberToShortStringCodec idCodec) {
    	this(new ContentTranslator(idCodec), idCodec);
    }
    
    public Item fromDB(DBObject dbObject) {
        return fromDBObject(dbObject, null);
    }
    
    @Override
    public Item fromDBObject(DBObject dbObject, Item item) {

        if (item == null) {
            item = (Item) DescribedTranslator.newModel(dbObject);
        }
        
        contentTranslator.fromDBObject(dbObject, item);
        
        item.setIsLongForm((Boolean) dbObject.get(IS_LONG_FORM_KEY));
        item.setBlackAndWhite(TranslatorUtils.toBoolean(dbObject, BLACK_AND_WHITE_KEY));
        Long duration = TranslatorUtils.toLong(dbObject, DURATION_KEY);
        if (duration != null) {
            item.setDuration(Duration.standardSeconds(duration));
        }
        if (dbObject.containsField(FILM_RELEASES_KEY)) {
            item.setReleaseDates(Iterables.transform(TranslatorUtils.toDBObjectList(dbObject, FILM_RELEASES_KEY), releaseDateFromDbo));
        }
        
        if(dbObject.containsField(CONTAINER)) {
            String containerUri = TranslatorUtils.toString(dbObject, CONTAINER);
            Long containerId = TranslatorUtils.toLong(dbObject, CONTAINER_ID);
            item.setParentRef(new ParentRef(containerUri, containerId));
        }

        if (item instanceof Episode) {
            Episode episode = (Episode) item;
            Long seriesId = TranslatorUtils.toLong(dbObject, SERIES_ID);
            if (dbObject.containsField(SERIES)) {
                String seriesUri = TranslatorUtils.toString(dbObject, SERIES);
                episode.setSeriesRef(new ParentRef(seriesUri, seriesId));
            }
            episode.setPartNumber(TranslatorUtils.toInteger(dbObject, PART_NUMBER));
            episode.setEpisodeNumber((Integer) dbObject.get(EPISODE_NUMBER));
            episode.setSeriesNumber((Integer) dbObject.get(SERIES_NUMBER));
            episode.setSpecial(TranslatorUtils.toBoolean(dbObject, SPECIAL));

            if (dbObject.containsField(EPISODE_SERIES_URI_KEY)) {
                episode.setSeriesRef(new ParentRef((String) dbObject.get(EPISODE_SERIES_URI_KEY),seriesId));
            }
        }
        
        if (item instanceof Film) {
            Film film = (Film) item;
            film.setWebsiteUrl(TranslatorUtils.toString(dbObject, FILM_WEBSITE_URL_KEY));
            if (dbObject.containsField(FILM_SUBTITLES_KEY)) {
                film.setSubtitles(Iterables.transform(TranslatorUtils.toDBObjectList(dbObject, FILM_SUBTITLES_KEY), subtitlesFromDbo));
            }
        }
        
        if (item instanceof Song) {
            Song song = (Song) item;
            song.setIsrc(TranslatorUtils.toString(dbObject, ISRC_KEY));
        }
        
        item.setReadHash(generateHashByRemovingFieldsFromTheDbo(dbObject));
        return item; 
    }
    
    public String hashCodeOf(Item item) {
        return generateHashByRemovingFieldsFromTheDbo(toDB(item));
    }

    private String generateHashByRemovingFieldsFromTheDbo(DBObject dbObject) {
        // don't include the last-fetched/update time and container/series ids in the hash
        removeFieldsForHash(dbObject);
        if (log.isTraceEnabled()) {
            dboHashCodeDebugger.logHashCodes(dbObject, log);
        }
        return String.valueOf(dbObject.hashCode());
    }

    @SuppressWarnings("unchecked")
    public void removeFieldsForHash(DBObject dbObject) {
        dbObject.removeField(CONTAINER_ID);
        dbObject.removeField(SERIES_ID);
        contentTranslator.removeFieldsForHash(dbObject);

    }




	
	public DBObject toDB(Item item) {
	    return toDBObject(null, item);
	}

    @Override
    public DBObject toDBObject(DBObject itemDbo, Item entity) {
        itemDbo = contentTranslator.toDBObject(itemDbo, entity);
        itemDbo.put(DescribedTranslator.TYPE_KEY, EntityType.from(entity).toString());
        encodeReleases(itemDbo, entity.getReleaseDates());
        
        itemDbo.put(IS_LONG_FORM_KEY, entity.getIsLongForm());

        TranslatorUtils.from(itemDbo, BLACK_AND_WHITE_KEY, entity.getBlackAndWhite());

        if (entity.getDuration() != null) {
            TranslatorUtils.from(itemDbo, DURATION_KEY, entity.getDuration().getStandardSeconds());
        }
		
        if(entity.getContainer() != null) {
            itemDbo.put(CONTAINER, entity.getContainer().getUri());
            itemDbo.put(CONTAINER_ID, entity.getContainer().getId());
        }

        if (entity instanceof Episode) {
			Episode episode = (Episode) entity;
			
			if(episode.getSeriesRef() != null) {
			    itemDbo.put(SERIES_ID, episode.getSeriesRef().getId());
			    itemDbo.put(SERIES, episode.getSeriesRef().getUri());
			}
			
			TranslatorUtils.from(itemDbo, PART_NUMBER, episode.getPartNumber());
			TranslatorUtils.from(itemDbo, EPISODE_NUMBER, episode.getEpisodeNumber());
			TranslatorUtils.from(itemDbo, SERIES_NUMBER, episode.getSeriesNumber());
			TranslatorUtils.from(itemDbo, SPECIAL, episode.getSpecial());

			ParentRef series = episode.getSeriesRef();
			if (series != null) {
				TranslatorUtils.from(itemDbo, EPISODE_SERIES_URI_KEY, series.getUri());
			}
		}
		
		if (entity instanceof Film) {
            Film film = (Film) entity;
            TranslatorUtils.from(itemDbo, FILM_WEBSITE_URL_KEY, film.getWebsiteUrl());

            encodeSubtitles(itemDbo, film.getSubtitles());
		}
		
        if (entity instanceof Song) {
            Song song = (Song) entity;
            TranslatorUtils.from(itemDbo, ISRC_KEY, song.getIsrc());
        }
		
        return itemDbo;
    }
    
    private void encodeReleases(DBObject dbo, Set<ReleaseDate> releaseDates) {
        if(!releaseDates.isEmpty()) {
            BasicDBList values = new BasicDBList();
            for(ReleaseDate releaseDate : releaseDates) {
                DBObject releaseDateDbo = new BasicDBObject();
                TranslatorUtils.fromLocalDate(releaseDateDbo, "date", releaseDate.date());
                TranslatorUtils.from(releaseDateDbo, "country", releaseDate.country().code());
                TranslatorUtils.from(releaseDateDbo, "type", releaseDate.type().toString());
                values.add(releaseDateDbo);
            }
            dbo.put(FILM_RELEASES_KEY, values);
        }        
    }

    private void encodeSubtitles(DBObject dbo, Set<Subtitles> subtitles) {
        if(!subtitles.isEmpty()) {
            BasicDBList values = new BasicDBList();
            for(Subtitles subtitle : subtitles) {
                DBObject subtitleDbo = new BasicDBObject();
                subtitleDbo.put("language", subtitle.code());
                values.add(subtitleDbo);
            }
            dbo.put(FILM_SUBTITLES_KEY, values);
        }
    }
    

    
}
