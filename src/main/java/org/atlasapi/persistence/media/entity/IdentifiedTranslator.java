package org.atlasapi.persistence.media.entity;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.cassandra.cli.CliParser.newColumnFamily_return;
import org.atlasapi.equiv.EquivalenceRef;
import org.atlasapi.media.common.Id;
import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.ModelTranslator;

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSet.Builder;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.metabroadcast.common.persistence.mongo.MongoConstants;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class IdentifiedTranslator implements ModelTranslator<Identified> {
   
	private static final Pattern URI_PREFIX = Pattern.compile("^[a-z]*:");

    public static final String CURIE = "curie";
	
	public static final String ALIASES = "aliases";
	public static final String IDS = "ids";
	public static final String LAST_UPDATED = "lastUpdated";
	public static final String EQUIVALENT_TO = "equivalent";
	public static final String ID = MongoConstants.ID;
	public static final String CANONICAL_URL = "uri";
	public static final String TYPE = "type";
    public static final String PUBLISHER = "publisher";
    public static final String OPAQUE_ID = "aid";
    
    private static final Map<String, String> NAMESPACE_TO_UPDATED_NAMESPACE_MAPPING = ImmutableMap.<String, String>builder()
        .put("youview:programme", "gb:youview:programme-id")
        .put("youview:scheduleevent", "gb:youview:scheduleevent-id")
        .put("dvb:pcrid", "gb:dvb:pcrid")
        .put("dvb:scrid", "gb:dvb:scrid")
        .put("pa:brand", "gb:pa:series-id")
        .put("pa:series", "gb:mbst:pa:season-id")
        .put("pa:episode", "gb:pa:prog-id")
        .put("pa:film", "gb:pa:prog-id")
        .build();
    
    private static final Map<String, String> ALIAS_NAMESPACE_MAPPING = ImmutableMap.<String, String>builder()
        // BBC
        .put("http://www\\.bbc\\.co\\.uk/programmes/([0-9a-z]*)", "gb:bbc:pid")
      //.put("http://wsarchive\\.bbc\\.co\\.uk/brands/([0-9]*)", "gb:bbc:pid")
      //.put("http://wsarchive\\.bbc\\.co\\.uk/episodes/([0-9]*)", "gb:bbc:pid")
        .put("http://bbc\\.co\\.uk/i/([a-z0-9]*)/", "gb:bbc:pid")
        .put("http://www\\.bbc\\.co\\.uk/iplayer/episode/([0-9a-z]*)", "gb:bbc:pid")
        .put("http://devapi\\.bbcredux\\.com/channels/([a-z0-9]*)", "gb:bbcredux:service-id")
        // PA
        .put("http://pressassociation\\.com/films/([0-9-]*)", "gb:pa:prog-id")
        .put("http://pressassociation\\.com/episodes/([0-9-]*)", "gb:pa:prog-id")
        .put("http://pressassociation\\.com/series/([0-9-]*)", "gb:mbst:pa:season-id")
        .put("http://pressassociation\\.com/brands/([0-9-]*)", "gb:pa:series-id")
        // PA Channels
        .put("http://pressassociation\\.com/channels/([0-9]*)", "gb:pa:channel")
        .put("http://pressassociation\\.com/stations/([0-9]*)", "gb:pa:station")
        .put("http://pressassociation\\.com/regions/([0-9]*)", "gb:pa:region")
        .put("http://pressassociation\\.com/platforms/([0-9]*)", "gb:pa:platform")
        // External channel aliases
        .put("http://youview\\.com/service/([0-9]*)", "gb:youview:service-id")
        .put("http://xmltv\\.radiotimes\\.com/channels/([0-9]*)", "gb:mbst:xmltv:channel-id")
        // LoveFilm
        .put("http://lovefilm\\.com/shows/([0-9]*)", "gb:lovefilm:sku")
        .put("http://lovefilm\\.com/seasons/([0-9]*)", "gb:lovefilm:sku")
        .put("http://lovefilm\\.com/episodes/([0-9]*)", "gb:lovefilm:sku")
        .put("http://lovefilm\\.com/films/([0-9]*)", "gb:lovefilm:sku")
        // YouView
        .put("http://youview\\.com/programme/([0-9]*)", "gb:youview:programme-id")
//        .put("(crid://[a-z0-9\\.]*/[0-9A-Z/]*)", "gb:dvb:crid")
        .put("(dvb://[a-z0-9\\.]*;[0-9a-z]*)", "gb:dvb:event-locator")
        .put("http://youview.com/scheduleevent/([0-9]*)", "gb:youview:scheduleevent-id")
        // IMDB
        .put("http://imdb\\.com/title/([a-z0-9]*)", "zz:imdb:id")
        .build();
    
    private final AliasTranslator aliasTranslator = new AliasTranslator();

	private boolean useAtlasIdAsId;
    
	public IdentifiedTranslator() {
		this(false);
	}
	
	public IdentifiedTranslator(boolean atlasIdAsId) {
		this.useAtlasIdAsId = atlasIdAsId;
	}
	
	@Override
    public Identified fromDBObject(DBObject dbObject, Identified description) {
        if (description == null) {
            description = new Identified();
        }
        
        if(useAtlasIdAsId) {
        	description.setCanonicalUri((String) dbObject.get(CANONICAL_URL));
        	description.setId((Long) dbObject.get(ID));
        }
        else {
        	description.setCanonicalUri((String) dbObject.get(ID));
        }
        
        description.setCurie((String) dbObject.get(CURIE));
        description.setAliasUrls(TranslatorUtils.toSet(dbObject, ALIASES));

        description.setAliases(updateAliases(aliasTranslator.fromDBObjects(TranslatorUtils.toDBObjectList(dbObject, IDS))));
        
        if (description.getCanonicalUri() != null) {
            description.addAliases(generateAliases(Sets.union(description.getAliasUrls(), ImmutableSet.of(description.getCanonicalUri()))));
        } else {
            description.addAliases(generateAliases(description.getAliasUrls()));    
        }
        
        description.setEquivalentTo(equivalentsFrom(dbObject));
        description.setLastUpdated(TranslatorUtils.toDateTime(dbObject, LAST_UPDATED));
        return description;
    }
	
    private Set<Alias> updateAliases(Set<Alias> aliases) {
        Set<Alias> newAliases = Sets.newHashSet();
        Set<Alias> oldAliases = Sets.newHashSet();
        for (Alias alias : aliases) {
            if (NAMESPACE_TO_UPDATED_NAMESPACE_MAPPING.get(alias.getNamespace()) != null) {
                newAliases.add(new Alias(NAMESPACE_TO_UPDATED_NAMESPACE_MAPPING.get(alias.getNamespace()), alias.getValue()));
                oldAliases.add(alias);
            }
        }
        if (!newAliases.isEmpty()) {
            for (Alias unchanged : Sets.difference(aliases, oldAliases)) {
                newAliases.add(unchanged);
            }
            return newAliases;
        }
        return aliases;
    }

    private Set<Alias> generateAliases(Set<String> aliasUrls) {
        Builder<Alias> aliases = ImmutableSet.builder(); 
        for (String aliasUrl : aliasUrls) {
            if (aliasUrl != null) {
                for (Entry<String, String> entry : ALIAS_NAMESPACE_MAPPING.entrySet()) {
                    Pattern p = Pattern.compile(entry.getKey());
                    Matcher m = p.matcher(aliasUrl);
                    if (m.matches()) {
                        aliases.add(new Alias(entry.getValue(), m.group(1)));
                    }
                }
                if (isUri(aliasUrl)) {
                    aliases.add(new Alias(Alias.URI_NAMESPACE, aliasUrl));
                }
            }
        }
        return aliases.build();
    }

    private boolean isUri(String aliasUrl) {
        return URI_PREFIX.matcher(aliasUrl).find();
    }

    private Set<EquivalenceRef> equivalentsFrom(DBObject dbObject) {
        return Sets.newHashSet(Iterables.filter(
                Iterables.transform(TranslatorUtils.toDBObjectList(dbObject, EQUIVALENT_TO), equivalentFromDbo), Predicates.notNull()));
    }
	
    private static final Function<DBObject, EquivalenceRef> equivalentFromDbo = new Function<DBObject, EquivalenceRef>() {
        @Override
        public EquivalenceRef apply(DBObject input) {
            Publisher publisher = Publisher.fromKey(TranslatorUtils.toString(input, PUBLISHER)).requireValue();
            Long aid = TranslatorUtils.toLong(input, OPAQUE_ID);
            return aid == null ? null 
                               : new EquivalenceRef(Id.valueOf(aid), publisher);
        }
    };

    @Override
    public DBObject toDBObject(DBObject dbObject, Identified entity) {
        if (dbObject == null) {
            dbObject = new BasicDBObject();
        }
        
        if (useAtlasIdAsId) {
            TranslatorUtils.from(dbObject, CANONICAL_URL, entity.getCanonicalUri());
            TranslatorUtils.from(dbObject, ID, entity.getId());
        } else {
            TranslatorUtils.from(dbObject, ID, entity.getCanonicalUri());
            TranslatorUtils.from(dbObject, OPAQUE_ID, entity.getId());
        }
        
        TranslatorUtils.from(dbObject, CURIE, entity.getCurie());
        TranslatorUtils.fromSet(dbObject, entity.getAliasUrls(), ALIASES);
        
        TranslatorUtils.from(dbObject, IDS, aliasTranslator.toDBList(entity.getAliases()));
        
        TranslatorUtils.from(dbObject, EQUIVALENT_TO, toDBObject(entity.getEquivalentTo()));
        TranslatorUtils.fromDateTime(dbObject, LAST_UPDATED, entity.getLastUpdated());
        
        return dbObject;
    }

    private BasicDBList toDBObject(Set<EquivalenceRef> equivalentTo) {
        BasicDBList list = new BasicDBList();
        Iterables.addAll(list, Iterables.transform(equivalentTo, equivalentToDbo));
        return list;
    }
    
    private static Function<EquivalenceRef, DBObject> equivalentToDbo = new Function<EquivalenceRef, DBObject>() {
        @Override
        public DBObject apply(EquivalenceRef input) {
            BasicDBObject dbo = new BasicDBObject();
            
            TranslatorUtils.from(dbo, OPAQUE_ID, input.getId().longValue());
            TranslatorUtils.from(dbo, PUBLISHER, input.getPublisher().key());
            
            return dbo;
        }
    };
}
