package org.atlasapi.persistence.audit;

import static com.google.common.base.Preconditions.checkNotNull;

import static com.metabroadcast.common.persistence.mongo.MongoBuilders.where;

import org.atlasapi.media.entity.Described;
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
        log(described, true);
    }

    @Override
    public void logNoWrite(Described described) {
        log(described, false);
    }
    
    private void log(Described described, boolean actualWrite) {
        this.collection.update(
                where()
                    .fieldEquals(PUBLISHER_KEY, described.getPublisher().key())
                    .fieldEquals(BUCKET_KEY, documentKey.apply(clock.now()))
                    .build(), 
                new MongoUpdateBuilder()
                    .incField(groupByKey(described, actualWrite), 1)
                    .build(),
                true,
                false);
    }
    
    private String groupByKey(Described described, boolean actualWrite) {
        
        return String.format("%s.%s",
                    actualWrite ? "write" : "noWrite",
                    described.getClass().getSimpleName().toLowerCase()
                );
    }

}
