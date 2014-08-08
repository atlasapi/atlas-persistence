package org.atlasapi.media.channel;

import java.util.Set;

import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.ModelTranslator;
import org.atlasapi.persistence.media.entity.IdentifiedTranslator;

import com.metabroadcast.common.intl.Countries;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
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
    public static final String CHANNEL_NUMBERINGS_KEY = "channelNumberings";
    public static final String CHANNELS_KEY = "channels";
    
    private final IdentifiedTranslator identifiedTranslator = new IdentifiedTranslator(true);
    private final ChannelNumberingTranslator channelNumberingTranslator = new ChannelNumberingTranslator();
    private final TemporalTitleTranslator temporalTitleTranslator = new TemporalTitleTranslator();
    
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
            TranslatorUtils.fromLongSet(dbObject, REGIONS_KEY, ((Platform)model).getRegions());
        } else if (model instanceof Region) {
            TranslatorUtils.from(dbObject, TYPE_KEY, REGION_VALUE);
            TranslatorUtils.from(dbObject, PLATFORM_KEY, ((Region)model).getPlatform());
        }
        
        temporalTitleTranslator.fromTemporalTitleSet(dbObject, TITLES_KEY, model.getAllTitles());
        
        if (model.getAvailableCountries() != null) {
            TranslatorUtils.fromSet(dbObject, Countries.toCodes(model.getAvailableCountries()), COUNTRIES_KEY);
        }
        
        if (model.getChannelNumberings() != null) {
            channelNumberingTranslator.fromChannelNumberingSet(dbObject, CHANNEL_NUMBERINGS_KEY, model.getChannelNumberings());
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
            ((Platform)model).setRegionIds(TranslatorUtils.toLongSet(dbObject, REGIONS_KEY));
            break;
        case REGION:
            if (model == null) {
                model = new Region();
            }
            ((Region)model).setPlatform(TranslatorUtils.toLong(dbObject, PLATFORM_KEY));
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
            model.setTitles(temporalTitleTranslator.toTemporalTitleSet(dbObject, TITLES_KEY));
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
                        .withChannel(channelId)
                        .withChannelGroup(model)
                        .build());
            }
        }
        
        if (dbObject.containsField(CHANNEL_NUMBERINGS_KEY)) {
            model.setChannelNumberings(channelNumberingTranslator.toChannelNumberingSet(dbObject, CHANNEL_NUMBERINGS_KEY));
        }
        
        return model;
    }
}
