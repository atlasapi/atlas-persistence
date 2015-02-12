package org.atlasapi.persistence.audit;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.metabroadcast.common.persistence.mongo.MongoBuilders.where;

import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.joda.time.DateTime;

import com.google.common.base.Function;
import com.metabroadcast.common.persistence.mongo.MongoUpdateBuilder;
import com.metabroadcast.common.time.Clock;
import com.mongodb.DBCollection;


class MongoPersistenceAuditLog implements PersistenceAuditLog {

    private static final String PUBLISHER_KEY = "publisher";
    private static final String BUCKET_KEY = "timeBucket";
    
    private final DBCollection collection;
    private final Function<DateTime, String> documentKey;
    private final Clock clock;

    public MongoPersistenceAuditLog(DBCollection collection, 
            Function<DateTime, String> documentKey, Clock clock) {
        this.collection = checkNotNull(collection);
        this.documentKey = checkNotNull(documentKey);
        this.clock = clock;
    }
    
    @Override
    public void logWrite(Described described) {
        log(described, described.getPublisher(), true);
    }
    
    @Override
    public void logWrite(LookupEntry lookupEntry) {
        log(lookupEntry, lookupEntry.lookupRef().publisher(), true);
    }

    @Override
    public void logNoWrite(Described described) {
        // Removing counting of this temporarily, to reduce oplog load
        // log(described, false);
    }
    
    @Override
    public void logNoWrite(LookupEntry lookupEntry) {
        //log(lookupEntry, true);
    }
    
    private void log(Object object, Publisher publisher, boolean actualWrite) {
        log(publisher, 
            groupByKey(object.getClass().getSimpleName().toLowerCase(), actualWrite));
    }
    
    private void log(Publisher publisher, String key) {
        this.collection.update(
                where()
                    .fieldEquals(PUBLISHER_KEY, publisher.key())
                    .fieldEquals(BUCKET_KEY, documentKey.apply(clock.now()))
                    .build(), 
                new MongoUpdateBuilder()
                    .incField(key, 1)
                    .build(),
                true,
                false);
    }
    
    private String groupByKey(String type, boolean actualWrite) {
        
        return String.format("%s.%s",
                    actualWrite ? "write" : "noWrite",
                    type
                );
    }

}
