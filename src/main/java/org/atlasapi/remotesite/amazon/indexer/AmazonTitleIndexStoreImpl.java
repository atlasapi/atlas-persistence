package org.atlasapi.remotesite.amazon.indexer;

import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.persistence.mongo.MongoBuilders;
import com.metabroadcast.common.persistence.mongo.MongoQueryBuilder;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import org.joda.time.DateTime;

import static com.metabroadcast.common.persistence.mongo.MongoConstants.SINGLE;
import static com.metabroadcast.common.persistence.mongo.MongoConstants.UPSERT;

//ENG-144
public class AmazonTitleIndexStoreImpl implements AmazonTitleIndexStore {
    private static final String AMAZON_TITLE_INDEX_COLLECTION = "amazonTitleIndex";

    private static final AmazonTitleIndexEntryTranslator amazonTitleIndexEntryTranslator = new AmazonTitleIndexEntryTranslator();

    private final DBCollection amazonTitleIndexCollection;

    public AmazonTitleIndexStoreImpl(DatabasedMongo mongo) {
        amazonTitleIndexCollection = mongo.collection(AMAZON_TITLE_INDEX_COLLECTION);
    }

    @Override
    public AmazonTitleIndexEntry createOrUpdateIndex(AmazonTitleIndexEntry amazonTitleIndexEntry) {
        amazonTitleIndexEntry.setLastUpdated(DateTime.now());
        DBObject dbObject = amazonTitleIndexEntryTranslator.toDBObject(amazonTitleIndexEntry);
        MongoQueryBuilder where = MongoBuilders.where()
                .idEquals(amazonTitleIndexEntry.getTitle())
                ;
        amazonTitleIndexCollection.update(where.build(), dbObject, UPSERT, SINGLE);
        return amazonTitleIndexEntry;
    }

    @Override
    public AmazonTitleIndexEntry getIndexEntry(String title) {
        MongoQueryBuilder where = MongoBuilders.where()
                .idEquals(title)
                ;
        DBObject dbObject = amazonTitleIndexCollection.findOne(where.build());
        return amazonTitleIndexEntryTranslator.fromDBObject(dbObject);
    }

}
