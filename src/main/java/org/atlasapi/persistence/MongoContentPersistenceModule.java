package org.atlasapi.persistence;

import org.atlasapi.media.segment.IdSettingSegmentWriter;
import org.atlasapi.media.segment.MongoSegmentResolver;
import org.atlasapi.media.segment.MongoSegmentWriter;
import org.atlasapi.media.segment.SegmentResolver;
import org.atlasapi.media.segment.SegmentWriter;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.KnownTypeContentResolver;
import org.atlasapi.persistence.content.LookupResolvingContentResolver;
import org.atlasapi.persistence.content.listing.ContentLister;
import org.atlasapi.persistence.content.mongo.MongoContentLister;
import org.atlasapi.persistence.content.mongo.MongoContentResolver;
import org.atlasapi.persistence.content.mongo.MongoContentWriter;
import org.atlasapi.persistence.content.mongo.MongoPersonStore;
import org.atlasapi.persistence.content.people.ItemsPeopleWriter;
import org.atlasapi.persistence.content.people.QueuingItemsPeopleWriter;
import org.atlasapi.persistence.content.people.QueuingPersonWriter;
import org.atlasapi.persistence.content.schedule.mongo.MongoScheduleStore;
import org.atlasapi.persistence.ids.MongoSequentialIdGenerator;
import org.atlasapi.persistence.logging.AdapterLog;
import org.atlasapi.persistence.lookup.mongo.MongoLookupEntryStore;
import org.atlasapi.persistence.shorturls.MongoShortUrlSaver;
import org.atlasapi.persistence.shorturls.ShortUrlSaver;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.persistence.mongo.health.MongoIOProbe;
import com.metabroadcast.common.time.SystemClock;
import com.mongodb.Mongo;
import com.mongodb.MongoReplicaSetProbe;
import com.mongodb.WriteConcern;

@Configuration
public class MongoContentPersistenceModule implements ContentPersistenceModule {

    private @Autowired Mongo mongo;
	private @Autowired DatabasedMongo db;
	private @Autowired AdapterLog log;
	
	
	public @Bean ContentWriter contentWriter() {
		return new MongoContentWriter(db, lookupStore(), new SystemClock());
	}
	
	@Primary
	public @Bean ContentResolver contentResolver() {
	    return new LookupResolvingContentResolver(knownTypeContentResolver(), lookupStore());
	}
	
	public @Bean KnownTypeContentResolver knownTypeContentResolver() {
	    return new MongoContentResolver(db);
	}
	
	public @Bean MongoLookupEntryStore lookupStore() {
	    return new MongoLookupEntryStore(db);
	}
	
	public @Bean MongoScheduleStore scheduleStore() {
	    try {
            return new MongoScheduleStore(db, contentResolver());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
	}
	
	public @Bean ItemsPeopleWriter itemsPeopleWriter() {
	    return new QueuingItemsPeopleWriter(new QueuingPersonWriter(personStore(), log), log);
	}
	
	public @Bean MongoPersonStore personStore() {
	    return new MongoPersonStore(db);
	}

	@Override
	public @Bean ShortUrlSaver shortUrlSaver() {
		return new MongoShortUrlSaver(db);
	}
	
	public @Bean ContentLister contentListener() {
		return new MongoContentLister(db);
	}
	
    @Bean
    MongoReplicaSetProbe mongoReplicaSetProbe() {
        return new MongoReplicaSetProbe(mongo);
    }

    @Bean
    MongoIOProbe mongoIoSetProbe() {
        return new MongoIOProbe(mongo).withWriteConcern(WriteConcern.REPLICAS_SAFE);
    }

    @Override
    public @Bean SegmentWriter segmentWriter() {
        return new IdSettingSegmentWriter(new MongoSegmentWriter(db, new SubstitutionTableNumberCodec()), segmentResolver(), new MongoSequentialIdGenerator(db, "segment"));
    }

    @Override
    public @Bean SegmentResolver segmentResolver() {
        return new MongoSegmentResolver(db, new SubstitutionTableNumberCodec());
    }
}
