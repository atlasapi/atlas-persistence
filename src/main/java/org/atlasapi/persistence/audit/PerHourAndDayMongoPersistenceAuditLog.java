package org.atlasapi.persistence.audit;

import java.util.Set;

import org.atlasapi.media.entity.Described;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.time.Clock;
import com.metabroadcast.common.time.SystemClock;


public class PerHourAndDayMongoPersistenceAuditLog implements PersistenceAuditLog {

    private static final String PER_DAY_LOG_COLLECTION = "perDayWriteLog";
    private static final String PER_HOUR_LOG_COLLECTION = "perHourWriteLog";
    private final Set<MongoPersistenceAuditLog> auditLogs;
    
    PerHourAndDayMongoPersistenceAuditLog(DatabasedMongo mongo, Clock clock) {
        this.auditLogs = ImmutableSet.of(
                new MongoPersistenceAuditLog(
                        mongo.collection(PER_DAY_LOG_COLLECTION),
                        PER_DAY_KEY_FUNCTION,
                        clock
                ),
                new MongoPersistenceAuditLog(
                        mongo.collection(PER_HOUR_LOG_COLLECTION),
                        PER_HOUR_KEY_FUNCTION,
                        clock)
             );
    }
    
    public PerHourAndDayMongoPersistenceAuditLog(DatabasedMongo mongo) {
        this(mongo, new SystemClock());
    }
    
    @Override
    public void logWrite(Described described) {
        for (MongoPersistenceAuditLog log : auditLogs) {
            log.logWrite(described);
        }
    }

    @Override
    public void logNoWrite(Described described) {
        for (MongoPersistenceAuditLog log : auditLogs) {
            log.logNoWrite(described);
        }
    }
    
    @Override
    public void logWrite(LookupEntry lookupEntry) {
        for (MongoPersistenceAuditLog log : auditLogs) {
            log.logWrite(lookupEntry);
        }
    }

    @Override
    public void logNoWrite(LookupEntry lookupEntry) {
        for (MongoPersistenceAuditLog log : auditLogs) {
            log.logNoWrite(lookupEntry);
        }
    }


    
    private static final DateTimeFormatter perDayFormatter = DateTimeFormat.forPattern("YYYY-MM-dd");
    private static Function<DateTime, String> PER_DAY_KEY_FUNCTION = new Function<DateTime, String>() {
        
        @Override
        public String apply(DateTime eventTime) {
            return perDayFormatter.print(eventTime);
        }
    };
    
    private static final DateTimeFormatter perHourFormatter = DateTimeFormat.forPattern("YYYY-MM-dd'T'HH");
    private static Function<DateTime, String> PER_HOUR_KEY_FUNCTION = new Function<DateTime, String>() {

        @Override
        public String apply(DateTime eventTime) {
            return perHourFormatter.print(eventTime);
        }
    };

}
