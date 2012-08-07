package org.atlasapi.persistence.media.entity;

import java.util.List;
import java.util.Set;

import org.atlasapi.media.entity.Certificate;
import org.atlasapi.media.entity.CrewMember;
import org.atlasapi.media.entity.EntityType;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Film;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.media.entity.ReleaseDate;
import org.atlasapi.media.entity.ReleaseDate.ReleaseType;
import org.atlasapi.media.entity.Subtitles;
import org.atlasapi.media.entity.Version;
import org.atlasapi.persistence.media.ModelTranslator;
import org.joda.time.LocalDate;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.intl.Countries;
import com.metabroadcast.common.intl.Country;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class ItemTranslator implements ModelTranslator<Item> {
    
    private static final String FILM_RELEASES_KEY = "releases";
    private static final String FILM_CERTIFICATES_KEY = "certificates";
    private static final String FILM_SUBTITLES_KEY = "subtitles";
    private static final String FILM_LANGUAGES_KEY = "languages";
    private static final String PART_NUMBER = "partNumber";
    private static final String EPISODE_NUMBER = "episodeNumber";
    private static final String SERIES_NUMBER = "seriesNumber";

    private static final String VERSIONS_KEY = "versions";
	private static final String TYPE_KEY = "type";
	private static final String IS_LONG_FORM_KEY = "isLongForm";
	private static final String EPISODE_SERIES_URI_KEY = "seriesUri";
	private static final String FILM_YEAR_KEY = "year";
	private static final String FILM_WEBSITE_URL_KEY = "websiteUrl";
	private static final String BLACK_AND_WHITE_KEY = "blackAndWhite";
	private static final String COUNTRIES_OF_ORIGIN_KEY = "countries";

	private final ContentTranslator contentTranslator;
    private final VersionTranslator versionTranslator;
    private final CrewMemberTranslator crewMemberTranslator = new CrewMemberTranslator();
    
    private final Function<DBObject, Subtitles> subtitlesFromDbo = new Function<DBObject, Subtitles>() {
        @Override
        public Subtitles apply(DBObject input) {
            return new Subtitles(TranslatorUtils.toString(input, "language"));
        }
    };
    private final Function<DBObject, Certificate> certificateFromDbo = new Function<DBObject, Certificate>() {
        @Override
        public Certificate apply(DBObject input) {
            return new Certificate(TranslatorUtils.toString(input, "class"),Countries.fromCode(TranslatorUtils.toString(input, "country")));
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
		this.versionTranslator = new VersionTranslator(idCodec);
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
        item.setCountriesOfOrigin(Countries.fromCodes(TranslatorUtils.toSet(dbObject, COUNTRIES_OF_ORIGIN_KEY)));
        
        List<DBObject> list = TranslatorUtils.toDBObjectList(dbObject, VERSIONS_KEY);
        if (list != null && ! list.isEmpty()) {
            Set<Version> versions = Sets.newHashSet();
            for (DBObject versionDbo: list) {
                if (versionDbo == null) {
                    throw new IllegalStateException("Cannot read item stored with null version: " + item.getCanonicalUri());
                }
                Version version = versionTranslator.fromDBObject(versionDbo, null);
                versions.add(version);
            }
            item.setVersions(versions);
        }
        
        list = TranslatorUtils.toDBObjectList(dbObject, "people");
        if (list != null && ! list.isEmpty()) {
            for (DBObject dbPerson: list) {
                CrewMember crewMember = crewMemberTranslator.fromDBObject(dbPerson, null);
                if (crewMember != null) {
                    item.addPerson(crewMember);
                }
            }
        }
        
        if(dbObject.containsField("container")) {
            item.setParentRef(new ParentRef((String)dbObject.get("container")));
        }

        if (item instanceof Episode) {
        	Episode episode = (Episode) item;
        	if(dbObject.containsField("series")) {
        	    episode.setSeriesRef(new ParentRef((String)dbObject.get("series")));
        	}
        	episode.setPartNumber(TranslatorUtils.toInteger(dbObject, PART_NUMBER));
        	episode.setEpisodeNumber((Integer) dbObject.get(EPISODE_NUMBER));
        	episode.setSeriesNumber((Integer) dbObject.get(SERIES_NUMBER));
        	if (dbObject.containsField(EPISODE_SERIES_URI_KEY)) {
        		episode.setSeriesRef(new ParentRef((String) dbObject.get(EPISODE_SERIES_URI_KEY)));
        	}
        }
        
        if (item instanceof Film) {
            Film film = (Film) item;
            film.setYear(TranslatorUtils.toInteger(dbObject, FILM_YEAR_KEY));
            film.setWebsiteUrl(TranslatorUtils.toString(dbObject, FILM_WEBSITE_URL_KEY));
            if (dbObject.containsField(FILM_LANGUAGES_KEY)) {
                film.setLanguages(TranslatorUtils.toSet(dbObject, FILM_LANGUAGES_KEY));
            }
            if (dbObject.containsField(FILM_SUBTITLES_KEY)) {
                film.setSubtitles(Iterables.transform(TranslatorUtils.toDBObjectList(dbObject, FILM_SUBTITLES_KEY), subtitlesFromDbo));
            }
            if (dbObject.containsField(FILM_LANGUAGES_KEY)) {
                film.setCertificates(Iterables.transform(TranslatorUtils.toDBObjectList(dbObject, FILM_CERTIFICATES_KEY), certificateFromDbo));
            } 
            if (dbObject.containsField(FILM_RELEASES_KEY)) {
                film.setReleaseDates(Iterables.transform(TranslatorUtils.toDBObjectList(dbObject, FILM_RELEASES_KEY), releaseDateFromDbo));
            }
        }
        
        item.setReadHash(generateHashByRemovingFieldsFromTheDbo(dbObject));
        return item; 
    }
    
    public String hashCodeOf(Item item) {
        return generateHashByRemovingFieldsFromTheDbo(toDB(item));
    }

    private String generateHashByRemovingFieldsFromTheDbo(DBObject dbObject) {
        // don't include the last-fetched/update time in the hash
        removeUpdateTimeFromItem(dbObject);

        return String.valueOf(dbObject.hashCode());
    }

    @SuppressWarnings("unchecked")
    public void removeUpdateTimeFromItem(DBObject dbObject) {
        dbObject.removeField(DescribedTranslator.LAST_FETCHED_KEY);
        dbObject.removeField(DescribedTranslator.THIS_OR_CHILD_LAST_UPDATED_KEY);
        dbObject.removeField(IdentifiedTranslator.LAST_UPDATED);

        Iterable<DBObject> versions = (Iterable<DBObject>) dbObject.get(VERSIONS_KEY);
        if (versions != null) {
            dbObject.put(VERSIONS_KEY, removeUpdateTimeFromVersions(versions));
        }
        Iterable<DBObject> clips = (Iterable<DBObject>) dbObject.get(ContentTranslator.CLIPS_KEY);
        if (clips != null) {
            Set<DBObject> unorderedClips = Sets.newHashSet();
            for (DBObject clipDbo : clips) {
                removeUpdateTimeFromItem(clipDbo);
                unorderedClips.add(clipDbo);
            }
            dbObject.put(ContentTranslator.CLIPS_KEY, unorderedClips);
        }
    }

    @SuppressWarnings("unchecked")
    private Set<DBObject> removeUpdateTimeFromVersions(Iterable<DBObject> versions) {
        Set<DBObject> unorderedVersions = Sets.newHashSet();
        for (DBObject versionDbo : versions) {
            versionDbo.removeField(IdentifiedTranslator.LAST_UPDATED);
            Iterable<DBObject> broadcasts = (Iterable<DBObject>) versionDbo.get(VersionTranslator.BROADCASTS_KEY);
            if (broadcasts != null) {
                Set<DBObject> unorderedBroadcasts = Sets.newHashSet();
                for (DBObject broadcastDbo : broadcasts) {
                    broadcastDbo.removeField(IdentifiedTranslator.LAST_UPDATED);
                    unorderedBroadcasts.add(broadcastDbo);
                }
                versionDbo.put(VersionTranslator.BROADCASTS_KEY, unorderedBroadcasts);
            }
            Iterable<DBObject> encodings = (Iterable<DBObject>) versionDbo.get(VersionTranslator.ENCODINGS_KEY);
            if (encodings != null) {
                versionDbo.put(VersionTranslator.ENCODINGS_KEY, removeUpdateTimesFromEncodings(encodings));
            }
            unorderedVersions.add(versionDbo);
        }
        return unorderedVersions;
    }

    @SuppressWarnings("unchecked")
    private Set<DBObject> removeUpdateTimesFromEncodings(Iterable<DBObject> encodings) {
        Set<DBObject> unorderedEncodings = Sets.newHashSet();
        for (DBObject encodingDbo : encodings) {
            encodingDbo.removeField(IdentifiedTranslator.LAST_UPDATED);
            Iterable<DBObject> locations = (Iterable<DBObject>) encodingDbo.get(EncodingTranslator.LOCATIONS_KEY);
            if (locations != null) {
                Set<DBObject> unorderedLocations = Sets.newHashSet();
                for (DBObject locationDbo : locations) {
                    locationDbo.removeField(IdentifiedTranslator.LAST_UPDATED);
                    DBObject policy = (DBObject) locationDbo.get(LocationTranslator.POLICY);
                    if(policy != null) {
                        policy.removeField(IdentifiedTranslator.LAST_UPDATED);
                    }
                    unorderedLocations.add(locationDbo);
                }
                encodingDbo.put(EncodingTranslator.LOCATIONS_KEY, unorderedLocations);
            }
            unorderedEncodings.add(encodingDbo);
        }
        return unorderedEncodings;
    }
	
	public DBObject toDB(Item item) {
	    return toDBObject(null, item);
	}

    @Override
    public DBObject toDBObject(DBObject itemDbo, Item entity) {
        itemDbo = contentTranslator.toDBObject(itemDbo, entity);
        itemDbo.put(TYPE_KEY, EntityType.from(entity).toString());
        
        itemDbo.put(IS_LONG_FORM_KEY, entity.getIsLongForm());
        if (! entity.getVersions().isEmpty()) {
            BasicDBList list = new BasicDBList();
            for (Version version: entity.getVersions()) {
                if (version == null) {
                    throw new IllegalArgumentException("Cannot save item with null version: " + entity.getCanonicalUri());
                }
                list.add(versionTranslator.toDBObject(null, version));
            }
            itemDbo.put(VERSIONS_KEY, list);
        }
        
        TranslatorUtils.from(itemDbo, BLACK_AND_WHITE_KEY, entity.getBlackAndWhite());
        if (! entity.getCountriesOfOrigin().isEmpty()) {
            TranslatorUtils.fromIterable(itemDbo, Countries.toCodes(entity.getCountriesOfOrigin()), COUNTRIES_OF_ORIGIN_KEY);
        }
		
        if(entity.getContainer() != null) {
            itemDbo.put("container", entity.getContainer().getUri());
        }

        if (entity instanceof Episode) {
			Episode episode = (Episode) entity;
			
			if(episode.getSeriesRef() != null) {
			    itemDbo.put("series", episode.getSeriesRef().getUri());
			}
			
			TranslatorUtils.from(itemDbo, PART_NUMBER, episode.getPartNumber());
			TranslatorUtils.from(itemDbo, EPISODE_NUMBER, episode.getEpisodeNumber());
			TranslatorUtils.from(itemDbo, SERIES_NUMBER, episode.getSeriesNumber());
			
			ParentRef series = episode.getSeriesRef();
			if (series != null) {
				TranslatorUtils.from(itemDbo, EPISODE_SERIES_URI_KEY, series.getUri());
			}
		}
		
		if (entity instanceof Film) {
		    Film film = (Film) entity;
		    TranslatorUtils.from(itemDbo, FILM_YEAR_KEY, film.getYear());
		    TranslatorUtils.from(itemDbo, FILM_WEBSITE_URL_KEY, film.getWebsiteUrl());
		    if (!film.getLanguages().isEmpty()) {
		        TranslatorUtils.fromSet(itemDbo, film.getLanguages(), FILM_LANGUAGES_KEY);
		    }
		    
		    encodeSubtitles(itemDbo, film.getSubtitles());
		    encodeReleases(itemDbo, film.getReleaseDates());
		    encodeCertificates(itemDbo, film.getCertificates());
		    
		}
		
		if (! entity.people().isEmpty()) {
		    BasicDBList list = new BasicDBList();
            for (CrewMember person: entity.people()) {
                list.add(crewMemberTranslator.toDBObject(null, person));
            }
            itemDbo.put("people", list);
		}
		
        return itemDbo;
    }

    private void encodeCertificates(DBObject dbo, Set<Certificate> certificates) {
        if(!certificates.isEmpty()) {
            BasicDBList values = new BasicDBList();
            for(Certificate releaseDate : certificates) {
                DBObject certDbo = new BasicDBObject();
                TranslatorUtils.from(certDbo, "class", releaseDate.classification());
                TranslatorUtils.from(certDbo, "country", releaseDate.country().code());
                values.add(certDbo);
            }
            dbo.put(FILM_CERTIFICATES_KEY, values);
        }  
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
