package org.atlasapi.remotesite.preview;

import com.google.common.collect.Iterables;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class MongoPreviewLastUpdatedStore implements PreviewLastUpdatedStore {

    private static final String LAST_UPDATED_COLLECTION = "lastUpdated";
    private static final String LAST_UPDATED_KEY = "lastUpdated";
    private final DBCollection lastUpdatedTimes;

    public MongoPreviewLastUpdatedStore(DatabasedMongo mongo) {
        lastUpdatedTimes = mongo.collection(LAST_UPDATED_COLLECTION);
    }
    
    @Override
    public void store(String lastUpdated) {
        DBObject dbo = new BasicDBObject();
        TranslatorUtils.from(dbo, LAST_UPDATED_KEY, lastUpdated);
        lastUpdatedTimes.update(new BasicDBObject(), dbo);
    }

    @Override
    public String retrieve() {
        DBCursor cursor = lastUpdatedTimes.find();
        if (cursor.size() == 0) {
            return null;
        }
        DBObject dbo = Iterables.getOnlyElement(cursor);
        return TranslatorUtils.toString(dbo, LAST_UPDATED_KEY);
    }

}
