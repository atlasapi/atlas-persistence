package org.atlasapi.persistence.media.entity;

import javax.annotation.Nullable;

import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Rating;

import com.google.common.base.Function;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.DBObject;


public class RatingsTranslator {

    private static String TYPE_KEY = "type";
    private static String VALUE_KEY = "value";
    
    private Rating fromDBObject(Publisher publisher, DBObject dbo) {
        return new Rating(
                TranslatorUtils.toString(dbo, TYPE_KEY),
                TranslatorUtils.toFloat(dbo, VALUE_KEY),
                publisher);
    }
    
    public DBObject toDBObject(DBObject dbObject, Rating model) {
        TranslatorUtils.from(dbObject, TYPE_KEY, model.getType());
        TranslatorUtils.from(dbObject, VALUE_KEY, model.getValue());
        // Publisher is not saved in the db. The only reason it's in the model is 
        // because it's output on merged content, so it's required in the merged
        // complex model.
        return dbObject;
    }
    
    public Function<DBObject, Rating> fromDbo(final Publisher publisher) {
        return new Function<DBObject, Rating>() {

            @Override
            @Nullable
            public Rating apply(@Nullable DBObject dbo) {
                return fromDBObject(publisher, dbo);
            }
        };
    }
}
