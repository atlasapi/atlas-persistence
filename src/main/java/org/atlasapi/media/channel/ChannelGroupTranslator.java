package org.atlasapi.media.channel;

import java.util.Set;

import org.atlasapi.media.channel.ChannelGroup.ChannelGroupType;
import org.atlasapi.media.content.Publisher;
import org.atlasapi.persistence.ModelTranslator;
import org.atlasapi.persistence.media.entity.IdentifiedTranslator;

import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.intl.Countries;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class ChannelGroupTranslator implements ModelTranslator<ChannelGroup>{

    private static final String TYPE_KEY = "type";
    private static final String SOURCE_KEY = "source";
    private static final String TITLE_KEY = "title";
    private static final String COUNTRIES_KEY = "countries";
    public static final String CHANNELS_KEY = "channels";
    
    private final IdentifiedTranslator identifiedTranslator = new IdentifiedTranslator(true);
    
    @Override
    public DBObject toDBObject(DBObject dbObject, ChannelGroup model) {
        if (dbObject == null) {
            dbObject = new BasicDBObject();
        }
        
        identifiedTranslator.toDBObject(dbObject, model);
        
        if (model.getPublisher() != null) {
            TranslatorUtils.from(dbObject, SOURCE_KEY, model.getPublisher().key());
        }
        
        TranslatorUtils.from(dbObject, TYPE_KEY, model.getType().key());
        
        TranslatorUtils.from(dbObject, TITLE_KEY, model.getTitle());
        
        if (model.getAvailableCountries() != null) {
            TranslatorUtils.fromSet(dbObject, Countries.toCodes(model.getAvailableCountries()), COUNTRIES_KEY);
        }
        
        if (model.getChannels() != null && !model.getChannels().isEmpty()) {
            dbObject.put(CHANNELS_KEY, model.getChannels());
        }
        
        return dbObject;
    }

    @Override
    @SuppressWarnings("unchecked")
    public ChannelGroup fromDBObject(DBObject dbObject, ChannelGroup model) {
        if (dbObject == null) {
            return null;
        }
        
        if (model == null) {
            model = new ChannelGroup();
        }
        
        identifiedTranslator.fromDBObject(dbObject, model);
        
        String source = TranslatorUtils.toString(dbObject, SOURCE_KEY);
        if (source != null) {
            model.setPublisher(Publisher.fromKey(source).valueOrNull());
        }
        
        model.setType(ChannelGroupType.fromKey(TranslatorUtils.toString(dbObject, TYPE_KEY)));
        
        model.setTitle(TranslatorUtils.toString(dbObject, TITLE_KEY));
        
        Set<String> countryCodes = TranslatorUtils.toSet(dbObject, COUNTRIES_KEY);
        if (countryCodes != null) {
            model.setAvailableCountries(Countries.fromCodes(countryCodes));
        }
        
        if (dbObject.containsField(CHANNELS_KEY)) {
            model.setChannels(ImmutableSet.copyOf((Iterable<Long>)dbObject.get(CHANNELS_KEY)));
        }
        
        return model;
    }

}
