package org.atlasapi.persistence.media.entity;

import java.util.List;
import java.util.Set;

import org.atlasapi.media.entity.CrewMember;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Series;
import org.atlasapi.media.entity.Version;
import org.atlasapi.persistence.ModelTranslator;

import com.google.common.base.Function;
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
	private static final String SYNTHETIC_KEY = "synthetic";
	private static final String EPISODE_SERIES_URI_KEY = "seriesUri";

	
	private final ContentTranslator contentTranslator;
    private final VersionTranslator versionTranslator = new VersionTranslator();
    private final CrewMemberTranslator crewMemberTranslator = new CrewMemberTranslator();
    
    ItemTranslator(ContentTranslator contentTranslator) {
		this.contentTranslator = contentTranslator;
    }
    
    public ItemTranslator() {
    	this(new ContentTranslator());
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
        
        list = (List) dbObject.get("people");
        if (list != null && ! list.isEmpty()) {
            for (DBObject dbPerson: list) {
                CrewMember crewMember = crewMemberTranslator.fromDBObject(dbPerson, null);
                if (crewMember != null) {
                    item.addPerson(crewMember);
                }
            }
        }

        if (item instanceof Episode) {
        	Episode episode = (Episode) item;
        	episode.setEpisodeNumber((Integer) dbObject.get("episodeNumber"));
        	episode.setSeriesNumber((Integer) dbObject.get("seriesNumber"));
        	if (dbObject.containsField(EPISODE_SERIES_URI_KEY)) {
        		episode.setSeriesUri((String) dbObject.get(EPISODE_SERIES_URI_KEY));
        	}
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
		
		if (entity instanceof Episode) {
			Episode episode = (Episode) entity;
			TranslatorUtils.from(itemDbo, "episodeNumber", episode.getEpisodeNumber());
			TranslatorUtils.from(itemDbo, "seriesNumber", episode.getSeriesNumber());
			Series series = episode.getSeries();
			if (series != null) {
				TranslatorUtils.from(itemDbo, EPISODE_SERIES_URI_KEY, series.getCanonicalUri());
			}
		}
		
		if (! entity.people().isEmpty()) {
		    BasicDBList list = new BasicDBList();
            for (CrewMember person: entity.people()) {
                list.add(crewMemberTranslator.toDBObject(null, person));
            }
            itemDbo.put("people", list);
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
