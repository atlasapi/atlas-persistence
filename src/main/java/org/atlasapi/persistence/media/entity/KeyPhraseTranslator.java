package org.atlasapi.persistence.media.entity;

import org.atlasapi.media.entity.KeyPhrase;
import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;

public class KeyPhraseTranslator {

    private static final String WEIGHTING_KEY = "weighting";
    private static final String PUBLISHER_KEY = "publisher";
    private static final String PHRASE_KEY = "phrase";

    public DBObject toDBObject(KeyPhrase entity) {
        
        DBObject dbo = BasicDBObjectBuilder.start().add(PHRASE_KEY, entity.getPhrase()).add(PUBLISHER_KEY, entity.getPublisher().key()).get();
        
        TranslatorUtils.from(dbo, WEIGHTING_KEY, entity.getWeighting());
        
        return dbo;
    }
    
    public KeyPhrase fromDBObject(DBObject dbo) {
        return new KeyPhrase(
                TranslatorUtils.toString(dbo, PHRASE_KEY), 
                Publisher.fromKey(TranslatorUtils.toString(dbo, PUBLISHER_KEY)).requireValue(), 
                TranslatorUtils.toDouble(dbo, WEIGHTING_KEY)
            );
    }
    
}
