package org.atlasapi.remotesite.preview;

import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class MongoPreviewLastUpdatedStore implements PreviewLastUpdatedStore {

    private static final String LAST_UPDATED_COLLECTION = "previewNetworksLastUpdated";
    private static final String LAST_UPDATED_KEY = "lastUpdated";
    private final DBCollection lastUpdatedId;

    public MongoPreviewLastUpdatedStore(DatabasedMongo mongo) {
        lastUpdatedId = mongo.collection(LAST_UPDATED_COLLECTION);
    }
    
    @Override
    public void store(String lastUpdated) {
        DBObject dbo = new BasicDBObject();
        TranslatorUtils.from(dbo, LAST_UPDATED_KEY, lastUpdated);
        lastUpdatedId.update(new BasicDBObject(), dbo);
    }

    @Override
    public Optional<String> retrieve() {
        DBCursor cursor = lastUpdatedId.find();
        if (cursor.size() == 0) {
            return Optional.absent();
        }
        DBObject dbo = Iterables.getOnlyElement(cursor);
        return Optional.fromNullable(TranslatorUtils.toString(dbo, LAST_UPDATED_KEY));
    }

}
