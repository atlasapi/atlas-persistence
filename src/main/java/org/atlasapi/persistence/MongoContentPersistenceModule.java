package org.atlasapi.persistence;

import org.atlasapi.media.channel.CachingChannelStore;
import org.atlasapi.media.channel.ChannelGroupStore;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.channel.ChannelStore;
import org.atlasapi.media.channel.MongoChannelGroupStore;
import org.atlasapi.media.channel.MongoChannelStore;
import org.atlasapi.media.product.IdSettingProductStore;
import org.atlasapi.media.product.ProductResolver;
import org.atlasapi.media.product.ProductStore;
import org.atlasapi.media.segment.IdSettingSegmentWriter;
import org.atlasapi.media.segment.MongoSegmentResolver;
import org.atlasapi.media.segment.MongoSegmentWriter;
import org.atlasapi.media.segment.SegmentResolver;
import org.atlasapi.media.segment.SegmentWriter;
import org.atlasapi.messaging.AtlasMessagingModule;
import org.atlasapi.persistence.content.ContentGroupResolver;
import org.atlasapi.persistence.content.ContentGroupWriter;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.DefaultEquivalentContentResolver;
import org.atlasapi.persistence.content.EquivalenceWritingContentWriter;
import org.atlasapi.persistence.content.EquivalentContentResolver;
import org.atlasapi.persistence.content.IdSettingContentWriter;
import org.atlasapi.persistence.content.KnownTypeContentResolver;
import org.atlasapi.persistence.content.LookupResolvingContentResolver;
import org.atlasapi.persistence.content.MessageQueueingContentWriter;
import org.atlasapi.persistence.content.PeopleQueryResolver;
import org.atlasapi.persistence.content.mongo.MongoContentGroupResolver;
import org.atlasapi.persistence.content.mongo.MongoContentGroupWriter;
import org.atlasapi.persistence.content.mongo.MongoContentLister;
import org.atlasapi.persistence.content.mongo.MongoContentResolver;
import org.atlasapi.persistence.content.mongo.MongoContentWriter;
import org.atlasapi.persistence.content.mongo.MongoPersonStore;
import org.atlasapi.persistence.content.mongo.MongoProductStore;
import org.atlasapi.persistence.content.mongo.MongoTopicStore;
import org.atlasapi.persistence.content.people.EquivalatingPeopleResolver;
import org.atlasapi.persistence.content.people.IdSettingPersonStore;
import org.atlasapi.persistence.content.people.ItemsPeopleWriter;
import org.atlasapi.persistence.content.people.PersonStore;
import org.atlasapi.persistence.content.people.QueuingItemsPeopleWriter;
import org.atlasapi.persistence.content.people.QueuingPersonWriter;
import org.atlasapi.persistence.content.schedule.mongo.MongoScheduleStore;
import org.atlasapi.persistence.ids.MongoSequentialIdGenerator;
import org.atlasapi.persistence.logging.AdapterLog;
import org.atlasapi.persistence.lookup.LookupWriter;
import org.atlasapi.persistence.lookup.MessageQueueingLookupWriter;
import org.atlasapi.persistence.lookup.TransitiveLookupWriter;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.atlasapi.persistence.lookup.mongo.MongoLookupEntryStore;
import org.atlasapi.persistence.shorturls.MongoShortUrlSaver;
import org.atlasapi.persistence.shorturls.ShortUrlSaver;
import org.atlasapi.persistence.topic.MessageQueueingTopicWriter;
import org.atlasapi.persistence.topic.TopicCreatingTopicResolver;
import org.atlasapi.persistence.topic.TopicQueryResolver;
import org.atlasapi.persistence.topic.TopicStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;

import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.persistence.mongo.health.MongoIOProbe;
import com.metabroadcast.common.properties.Configurer;
import com.metabroadcast.common.properties.Parameter;
import com.metabroadcast.common.time.SystemClock;
import com.mongodb.Mongo;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;

@Configuration
@Import(AtlasMessagingModule.class)
public class MongoContentPersistenceModule implements ContentPersistenceModule {

    private @Autowired Mongo mongo;
    private @Autowired DatabasedMongo db;
    private @Autowired AdapterLog log;
    private @Autowired AtlasMessagingModule messagingModule;
    
    private final Parameter processingConfig = Configurer.get("processing.config");
    
    private @Value("${ids.generate}") String generateIds;
    
    public MongoContentPersistenceModule() {}
    
    public MongoContentPersistenceModule(Mongo mongo, DatabasedMongo db, AtlasMessagingModule messagingModule, AdapterLog log) {
        this.mongo = mongo;
        this.db = db;
        this.log = log;
        this.messagingModule = messagingModule;
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
        contentWriter = new EquivalenceWritingContentWriter(contentWriter, explicitLookupWriter());
        contentWriter = new MessageQueueingContentWriter(messagingModule.contentChanges(), contentWriter);
        if (Boolean.valueOf(generateIds)) {
            contentWriter = new IdSettingContentWriter(lookupStore(), new MongoSequentialIdGenerator(db, "content"), contentWriter);
        }
        return contentWriter;
    }

    public @Primary @Bean ContentResolver contentResolver() {
        return new LookupResolvingContentResolver(knownTypeContentResolver(), lookupStore());
    }
    
    public @Primary @Bean KnownTypeContentResolver knownTypeContentResolver() {
        return new MongoContentResolver(db, lookupStore());
    }
    
    public @Primary @Bean MongoLookupEntryStore lookupStore() {
        return new MongoLookupEntryStore(db.collection("lookup"));
    }
    
	private LookupWriter explicitLookupWriter() {
        MongoLookupEntryStore entryStore = new MongoLookupEntryStore(db.collection("lookup"), ReadPreference.primary());
        LookupWriter lookupWriter = TransitiveLookupWriter.explicitTransitiveLookupWriter(entryStore);
        return messagingLookupWriter(lookupWriter);
    }
	
	public @Bean LookupWriter generatedLookupWriter() {
	    MongoLookupEntryStore entryStore = new MongoLookupEntryStore(db.collection("lookup"), ReadPreference.primary());
	    LookupWriter lookupWriter = TransitiveLookupWriter.generatedTransitiveLookupWriter(entryStore);
        return messagingLookupWriter(lookupWriter);
	}
	
	private MessageQueueingLookupWriter messagingLookupWriter(LookupWriter lookupWriter) {
	    return new MessageQueueingLookupWriter(messagingModule.equivChanges(), lookupWriter);
	}
	
	public @Primary @Bean MongoScheduleStore scheduleStore() {
	    try {
            return new MongoScheduleStore(db, contentResolver(), channelResolver, equivContentResolver());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public @Primary @Bean EquivalentContentResolver equivContentResolver() {
        return new DefaultEquivalentContentResolver(knownTypeContentResolver(), lookupStore());
    }
    
    public @Primary @Bean ItemsPeopleWriter itemsPeopleWriter() {
        return new QueuingItemsPeopleWriter(new QueuingPersonWriter(personStore(), log), log);
    }
    
    public @Primary @Bean PersonStore personStore() {
        LookupEntryStore personLookupEntryStore = new MongoLookupEntryStore(db.collection("peopleLookup"));
        PersonStore personStore = new MongoPersonStore(db, TransitiveLookupWriter.explicitTransitiveLookupWriter(personLookupEntryStore), personLookupEntryStore);
        if (Boolean.valueOf(generateIds)) {
            //For now people occupy the same id space as content.
            personStore = new IdSettingPersonStore(personStore, new MongoSequentialIdGenerator(db, "content"));
        }
        return personStore;
    }

    public @Primary @Bean ShortUrlSaver shortUrlSaver() {
        return new MongoShortUrlSaver(db);
    }
    
    public @Primary @Bean MongoContentLister contentLister() {
        return new MongoContentLister(db);
    }

    public @Primary @Bean TopicStore topicStore() {
        return new TopicCreatingTopicResolver(
                new MessageQueueingTopicWriter(messagingModule.topicChanges(), new MongoTopicStore(db)),
                new MongoSequentialIdGenerator(db, "topic"));
    }

    public @Primary @Bean TopicQueryResolver topicQueryResolver() {
        return new MongoTopicStore(db);
    }
    
    public @Primary @Bean SegmentWriter segmentWriter() {
        return new IdSettingSegmentWriter(new MongoSegmentWriter(db, new SubstitutionTableNumberCodec()), segmentResolver(), new MongoSequentialIdGenerator(db, "segment"));
    }

    public @Primary @Bean SegmentResolver segmentResolver() {
        return new MongoSegmentResolver(db, new SubstitutionTableNumberCodec());
    }
        
    public @Primary @Bean ChannelStore channelStore() {
        if (processingConfig == null || !processingConfig.toBoolean()) {
            return new CachingChannelStore(new MongoChannelStore(db, channelGroupStore(), channelGroupStore()));
        }
        return new MongoChannelStore(db, channelGroupStore(), channelGroupStore());
    }
    
    public @Primary @Bean ChannelGroupStore channelGroupStore() {
        return new MongoChannelGroupStore(db);
    }
    
    public @Primary @Bean ProductStore productStore() {
        return new IdSettingProductStore((ProductStore)productResolver(), new MongoSequentialIdGenerator(db, "product"));
    }

    public @Primary @Bean ProductResolver productResolver() {
        return new MongoProductStore(db);
    }
    
    @Bean
    MongoIOProbe mongoIoSetProbe() {
        return new MongoIOProbe(mongo).withWriteConcern(WriteConcern.REPLICAS_SAFE);
    }
    
    public @Bean PeopleQueryResolver peopleQueryResolver() {
        return new EquivalatingPeopleResolver(personStore(), new MongoLookupEntryStore(db.collection("peopleLookup")));
    }
}
