package org.atlasapi.remotesite.amazon.indexer;

import com.metabroadcast.common.persistence.mongo.MongoConstants;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.joda.time.DateTime;

import java.util.Set;

public class AmazonTitleIndexEntryTranslator {
    private static final String ID = MongoConstants.ID;
    private static final String URIS = "uris";
    private static final String LAST_UPDATED = "lastUpdated";

    public AmazonTitleIndexEntryTranslator() {

    }

    public DBObject toDBObject(AmazonTitleIndexEntry amazonTitleIndexEntry) {
        DBObject dbObject = new BasicDBObject();
        dbObject.put(ID, amazonTitleIndexEntry.getTitle());
        TranslatorUtils.fromSet(dbObject, amazonTitleIndexEntry.getUris(), URIS);
        TranslatorUtils.fromDateTime(
                dbObject,
                LAST_UPDATED,
                amazonTitleIndexEntry.getLastUpdated()
        );
        return dbObject;
    }

    public AmazonTitleIndexEntry fromDBObject(DBObject dbObject) {
        String title = TranslatorUtils.toString(dbObject, ID);
        Set<String> uris = TranslatorUtils.toSet(dbObject, URIS);
        DateTime lastUpdated = TranslatorUtils.toDateTime(dbObject, LAST_UPDATED);
        return new AmazonTitleIndexEntry(
                title,
                uris,
                lastUpdated
        );
    }
}
