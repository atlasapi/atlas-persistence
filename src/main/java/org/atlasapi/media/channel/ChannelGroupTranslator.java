package org.atlasapi.media.channel;

import java.util.List;
import java.util.Set;

import org.atlasapi.media.common.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.ModelTranslator;
import org.atlasapi.persistence.media.entity.IdentifiedTranslator;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.metabroadcast.common.intl.Countries;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class ChannelGroupTranslator implements ModelTranslator<ChannelGroup>{

    private static final String TYPE_KEY = "type";
    private static final String PLATFORM_VALUE = "platform";
    private static final String REGION_VALUE = "region";
    private static final String SOURCE_KEY = "source";
    private static final String TITLE_KEY = "title";
    private static final String TITLES_KEY = "titles";
    private static final String COUNTRIES_KEY = "countries";
    private static final String REGIONS_KEY = "regions";
    private static final String PLATFORM_KEY = "platform";
    private static final String CHANNEL_NUMBERINGS_KEY = "channelNumberings";
    public static final String CHANNELS_KEY = "channels";
    
    private final IdentifiedTranslator identifiedTranslator = new IdentifiedTranslator(true);
    private final ChannelNumberingTranslator channelNumberingTranslator = new ChannelNumberingTranslator();
    private final TemporalStringTranslator temporalStringTranslator = new TemporalStringTranslator();
    
    @Override
    public DBObject toDBObject(DBObject dbObject, ChannelGroup model) {
        if (dbObject == null) {
            dbObject = new BasicDBObject();
        }
        
        identifiedTranslator.toDBObject(dbObject, model);
        
        if (model.getPublisher() != null) {
            TranslatorUtils.from(dbObject, SOURCE_KEY, model.getPublisher().key());
        }
        
        if (model instanceof Platform) {
            TranslatorUtils.from(dbObject, TYPE_KEY, PLATFORM_VALUE);
            TranslatorUtils.fromLongSet(dbObject, REGIONS_KEY, ImmutableSet.copyOf(Iterables.transform(((Platform)model).getRegions(), Id.toLongValue())));
        } else if (model instanceof Region) {
            TranslatorUtils.from(dbObject, TYPE_KEY, REGION_VALUE);
            Region region = (Region) model;
            if (region.getPlatform() != null) {
                TranslatorUtils.from(dbObject, PLATFORM_KEY, region.getPlatform().longValue());
            }
        }
        
        fromTemporalStringSet(dbObject, TITLES_KEY, model.getAllTitles());
        
        if (model.getAvailableCountries() != null) {
            TranslatorUtils.fromSet(dbObject, Countries.toCodes(model.getAvailableCountries()), COUNTRIES_KEY);
        }
        
        if (model.getChannelNumberings() != null) {
            fromChannelNumberingSet(dbObject, CHANNEL_NUMBERINGS_KEY, model.getChannelNumberings());
        }
        
        return dbObject;
    }

    @SuppressWarnings("unchecked")
    @Override
    public ChannelGroup fromDBObject(DBObject dbObject, ChannelGroup model) {
        if (dbObject == null) {
            return null;
        }
        
        if (!dbObject.containsField(TYPE_KEY)) {
            throw new IllegalStateException("Missing type field");
        }
        
        ChannelGroupType type = ChannelGroupType.from(TranslatorUtils.toString(dbObject, TYPE_KEY));
        
        switch(type) {
        case PLATFORM:
            if (model == null) {
                model = new Platform();
            }
            ((Platform)model).setRegionIds(Iterables.transform(TranslatorUtils.toLongSet(dbObject, REGIONS_KEY), Id.fromLongValue()));
            break;
        case REGION:
            if (model == null) {
                model = new Region();
            }
            if (dbObject.containsField(PLATFORM_KEY)) {
                ((Region)model).setPlatform(Id.valueOf(TranslatorUtils.toLong(dbObject, PLATFORM_KEY)));
            }
            break;
        default:
            throw new IllegalArgumentException("Unknown type: " + type);                    
        }
        
        identifiedTranslator.fromDBObject(dbObject, model);
        
        String source = TranslatorUtils.toString(dbObject, SOURCE_KEY);
        if (source != null) {
            model.setPublisher(Publisher.fromKey(source).valueOrNull());
        }
        if (dbObject.containsField(TITLES_KEY)) {
            model.setTitles(toTemporalStringSet(dbObject, TITLES_KEY));
        }
        // if there is an old style title, retrieve it and add it to the temporal set
        if (dbObject.containsField(TITLE_KEY)) {
            model.addTitle(TranslatorUtils.toString(dbObject, TITLE_KEY));
        }
        
        Set<String> countryCodes = TranslatorUtils.toSet(dbObject, COUNTRIES_KEY);
        if (countryCodes != null) {
            model.setAvailableCountries(Countries.fromCodes(countryCodes));
        }
        
        if (dbObject.containsField(CHANNELS_KEY)) {
            // convert existing channel ids to channelnumberings with null channel numbers
            for (Long channelId :(Iterable<Long>)dbObject.get(CHANNELS_KEY)) {
                model.addChannelNumbering(ChannelNumbering.builder()
                        .withChannel(Id.valueOf(channelId))
                        .withChannelGroup(model)
                        .build());
            }
        }
        
        if (dbObject.containsField(CHANNEL_NUMBERINGS_KEY)) {
            model.setChannelNumberings(toChannelNumberingSet(dbObject, CHANNEL_NUMBERINGS_KEY));
        }
        
        return model;
    }
    
    private void fromChannelNumberingSet(DBObject dbObject, String key, Set<ChannelNumbering> set) {
        BasicDBList values = new BasicDBList();
        for (ChannelNumbering value : set) {
            if (value != null) {
                values.add(channelNumberingTranslator.toDBObject(value));
            }
        }
        dbObject.put(key, values);
    }
    
    @SuppressWarnings("unchecked")
    private Set<ChannelNumbering> toChannelNumberingSet(DBObject object, String name) {
        Set<ChannelNumbering> channelNumbers = Sets.newLinkedHashSet();
        if (object.containsField(name)) {
            for (DBObject element : (List<DBObject>) object.get(name)) {
                channelNumbers.add(channelNumberingTranslator.fromDBObject(element));
            }
            return channelNumbers;
        }
        return Sets.newLinkedHashSet();
    }
    
    private void fromTemporalStringSet(DBObject dbObject, String key, Iterable<TemporalString> iterable) {
        BasicDBList values = new BasicDBList();
        for (TemporalString value : iterable) {
            if (value != null) {
                values.add(temporalStringTranslator.toDBObject(value));
            }
        }
        dbObject.put(key, values);
    }
    
    @SuppressWarnings("unchecked")
    private Set<TemporalString> toTemporalStringSet(DBObject object, String name) {
        if (object.containsField(name)) {
            Set<TemporalString> temporalString = Sets.newLinkedHashSet();
            for (DBObject element : (List<DBObject>) object.get(name)) {
                temporalString.add(temporalStringTranslator.fromDBObject(element));
            }
            return temporalString;
        }
        return Sets.newLinkedHashSet();
    }
}
