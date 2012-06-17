package org.atlasapi.persistence;

import org.atlasapi.media.channel.ChannelGroupStore;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.channel.MongoChannelGroupStore;
import org.atlasapi.media.channel.MongoChannelStore;
import org.atlasapi.media.content.ContentGroupResolver;
import org.atlasapi.media.content.ContentGroupWriter;
import org.atlasapi.media.content.ContentResolver;
import org.atlasapi.media.content.ContentWriter;
import org.atlasapi.media.content.MongoContentLister;
import org.atlasapi.media.content.MongoContentResolver;
import org.atlasapi.media.content.MongoContentWriter;
import org.atlasapi.media.content.MongoPersonStore;
import org.atlasapi.media.content.MongoProductStore;
import org.atlasapi.media.content.MongoTopicStore;
import org.atlasapi.media.content.util.EquivalenceWritingContentWriter;
import org.atlasapi.media.content.util.IdSettingContentWriter;
import org.atlasapi.media.content.util.KnownTypeContentResolver;
import org.atlasapi.media.content.util.LookupResolvingContentResolver;
import org.atlasapi.media.product.IdSettingProductStore;
import org.atlasapi.media.product.ProductResolver;
import org.atlasapi.media.product.ProductStore;
import org.atlasapi.media.segment.IdSettingSegmentWriter;
import org.atlasapi.media.segment.MongoSegmentResolver;
import org.atlasapi.media.segment.MongoSegmentWriter;
import org.atlasapi.media.segment.SegmentResolver;
import org.atlasapi.media.segment.SegmentWriter;
import org.atlasapi.media.topic.TopicQueryResolver;
import org.atlasapi.media.topic.TopicStore;
import org.atlasapi.persistence.content.mongo.MongoContentGroupResolver;
import org.atlasapi.persistence.content.mongo.MongoContentGroupWriter;
import org.atlasapi.persistence.content.people.ItemsPeopleWriter;
import org.atlasapi.persistence.content.people.QueuingItemsPeopleWriter;
import org.atlasapi.persistence.content.people.QueuingPersonWriter;
import org.atlasapi.persistence.content.schedule.MongoScheduleStore;
import org.atlasapi.persistence.ids.MongoSequentialIdGenerator;
import org.atlasapi.persistence.logging.AdapterLog;
import org.atlasapi.persistence.lookup.MongoLookupEntryStore;
import org.atlasapi.persistence.shorturls.MongoShortUrlSaver;
import org.atlasapi.persistence.shorturls.ShortUrlSaver;
import org.atlasapi.persistence.topic.util.TopicCreatingTopicResolver;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
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
	
	private @Value("${ids.generate}") String generateIds;
	
	public MongoContentPersistenceModule() {}
	
	public MongoContentPersistenceModule(Mongo mongo, DatabasedMongo db, AdapterLog log) {
        this.mongo = mongo;
        this.db = db;
        this.log = log;
    }
	
	private @Autowired ChannelResolver channelResolver;
    
    public @Bean ContentGroupWriter contentGroupWriter() {
		ContentGroupWriter contentGroupWriter = new MongoContentGroupWriter(db, new SystemClock());
		return contentGroupWriter;
	}
	
	public @Bean ContentGroupResolver contentGroupResolver() {
	    return new MongoContentGroupResolver(db);
	}
	
	public @Primary @Bean ContentWriter contentWriter() {
		ContentWriter contentWriter = new MongoContentWriter(db, lookupStore(), new SystemClock());
		contentWriter = new EquivalenceWritingContentWriter(contentWriter, lookupStore());
		if (Boolean.valueOf(generateIds)) {
		    contentWriter = new IdSettingContentWriter(lookupStore(), new MongoSequentialIdGenerator(db, "content"), contentWriter);
		}
        return contentWriter;
	}

	public @Primary @Bean ContentResolver contentResolver() {
	    return new LookupResolvingContentResolver(knownTypeContentResolver(), lookupStore());
	}
	
	public @Primary @Bean KnownTypeContentResolver knownTypeContentResolver() {
	    return new MongoContentResolver(db);
	}
	
	public @Primary @Bean MongoLookupEntryStore lookupStore() {
	    return new MongoLookupEntryStore(db);
	}
	
	public @Primary @Bean MongoScheduleStore scheduleStore() {
	    try {
            return new MongoScheduleStore(db, contentResolver(), channelResolver);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
	}
	
	public @Primary @Bean ItemsPeopleWriter itemsPeopleWriter() {
	    return new QueuingItemsPeopleWriter(new QueuingPersonWriter(personStore(), log), log);
	}
	
	public @Primary @Bean MongoPersonStore personStore() {
	    return new MongoPersonStore(db);
	}

	public @Primary @Bean ShortUrlSaver shortUrlSaver() {
		return new MongoShortUrlSaver(db);
	}
	
	public @Primary @Bean MongoContentLister contentLister() {
		return new MongoContentLister(db);
    }

    public @Primary @Bean TopicStore topicStore() {
        return new TopicCreatingTopicResolver(new MongoTopicStore(db), new MongoSequentialIdGenerator(db, "topic"));
    }

    public @Primary @Bean TopicQueryResolver topicQueryResolver() {
        return new MongoTopicStore(db);
    }
    
    @Override
    public @Primary @Bean SegmentWriter segmentWriter() {
        return new IdSettingSegmentWriter(new MongoSegmentWriter(db, new SubstitutionTableNumberCodec()), segmentResolver(), new MongoSequentialIdGenerator(db, "segment"));
    }

    public @Primary @Bean SegmentResolver segmentResolver() {
        return new MongoSegmentResolver(db, new SubstitutionTableNumberCodec());
    }
        
    public @Primary @Bean ChannelResolver channelResolver() {
    	return new MongoChannelStore(db);
    }
    
    public @Primary @Bean ChannelGroupStore channelGroupStore() {
        return new MongoChannelGroupStore(db);
    }
    
    @Override
    public @Primary @Bean ProductStore productStore() {
        return new IdSettingProductStore((ProductStore)productResolver(), new MongoSequentialIdGenerator(db, "product"));
    }
    
    @Override
    public @Primary @Bean ProductResolver productResolver() {
        return new MongoProductStore(db);
    }
    
    @Bean
    MongoReplicaSetProbe mongoReplicaSetProbe() {
        return new MongoReplicaSetProbe(mongo);
    }

    @Bean
    MongoIOProbe mongoIoSetProbe() {
        return new MongoIOProbe(mongo).withWriteConcern(WriteConcern.REPLICAS_SAFE);
    }
}
