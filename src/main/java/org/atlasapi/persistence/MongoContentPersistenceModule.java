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
import org.atlasapi.messaging.v3.EntityUpdatedMessage;
import org.atlasapi.messaging.v3.JacksonMessageSerializer;
import org.atlasapi.messaging.v3.KafkaMessagingModule;
import org.atlasapi.messaging.v3.MessagingModule;
import org.atlasapi.messaging.v3.ScheduleUpdateMessage;
import org.atlasapi.persistence.audit.PerHourAndDayMongoPersistenceAuditLog;
import org.atlasapi.persistence.audit.PersistenceAuditLog;
import org.atlasapi.persistence.content.ContentGroupResolver;
import org.atlasapi.persistence.content.ContentGroupWriter;
import org.atlasapi.persistence.content.ContentPurger;
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
import org.atlasapi.persistence.content.mongo.MongoContentPurger;
import org.atlasapi.persistence.content.mongo.MongoContentResolver;
import org.atlasapi.persistence.content.mongo.MongoContentTables;
import org.atlasapi.persistence.content.mongo.MongoContentWriter;
import org.atlasapi.persistence.content.mongo.MongoPersonStore;
import org.atlasapi.persistence.content.mongo.MongoPlayerStore;
import org.atlasapi.persistence.content.mongo.MongoProductStore;
import org.atlasapi.persistence.content.mongo.MongoServiceStore;
import org.atlasapi.persistence.content.mongo.MongoTopicStore;
import org.atlasapi.persistence.content.organisation.IdSettingOrganisationStore;
import org.atlasapi.persistence.content.organisation.MongoOrganisationStore;
import org.atlasapi.persistence.content.organisation.OrganisationStore;
import org.atlasapi.persistence.content.people.EquivalatingPeopleResolver;
import org.atlasapi.persistence.content.people.IdSettingPersonStore;
import org.atlasapi.persistence.content.people.ItemsPeopleWriter;
import org.atlasapi.persistence.content.people.PersonStore;
import org.atlasapi.persistence.content.people.QueuingItemsPeopleWriter;
import org.atlasapi.persistence.content.people.QueuingPersonWriter;
import org.atlasapi.persistence.content.schedule.mongo.MongoScheduleStore;
import org.atlasapi.persistence.event.EventStore;
import org.atlasapi.persistence.event.IdSettingEventStore;
import org.atlasapi.persistence.event.MongoEventStore;
import org.atlasapi.persistence.ids.MongoSequentialIdGenerator;
import org.atlasapi.persistence.logging.AdapterLog;
import org.atlasapi.persistence.lookup.LookupWriter;
import org.atlasapi.persistence.lookup.TransitiveLookupWriter;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.atlasapi.persistence.lookup.mongo.MongoLookupEntryStore;
import org.atlasapi.persistence.player.CachingPlayerResolver;
import org.atlasapi.persistence.player.PlayerResolver;
import org.atlasapi.persistence.service.CachingServiceResolver;
import org.atlasapi.persistence.service.ServiceResolver;
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
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Primary;

import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.persistence.mongo.health.MongoIOProbe;
import com.metabroadcast.common.properties.Configurer;
import com.metabroadcast.common.properties.Parameter;
import com.metabroadcast.common.queue.MessageSender;
import com.metabroadcast.common.time.SystemClock;
import com.mongodb.Mongo;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;

@Configuration
@Import(KafkaMessagingModule.class)
public class MongoContentPersistenceModule implements ContentPersistenceModule {

    private @Autowired Mongo mongo;
    private @Autowired DatabasedMongo db;
    private @Autowired AdapterLog log;
    private @Autowired MessagingModule messagingModule;
    
    private final Parameter processingConfig = Configurer.get("processing.config");
    
    private @Value("${messaging.destination.content.changes}") String contentChanges;
    private @Value("${messaging.destination.topics.changes}") String topicChanges;
    private @Value("${messaging.destination.schedule.changes}") String scheduleChanges;
    private @Value("${ids.generate}") String generateIds;
    private @Value("${messaging.enabled}") String messagingEnabled;
    private @Value("${mongo.auditDbName}") String auditDbName;
    public MongoContentPersistenceModule() {}
    
    public MongoContentPersistenceModule(Mongo mongo, DatabasedMongo db, MessagingModule messagingModule, String auditDbName, AdapterLog log) {
        this.mongo = mongo;
        this.db = db;
        this.log = log;
        this.messagingModule = messagingModule;
        this.generateIds = "true";
        this.auditDbName = auditDbName;
    }
    
    @Bean
    @Lazy(true)
    public MessageSender<EntityUpdatedMessage> contentChanges() {
        return messagingModule.messageSenderFactory().makeMessageSender(contentChanges,
                JacksonMessageSerializer.forType(EntityUpdatedMessage.class));
    }
    
    @Bean
    @Lazy(true)
    public MessageSender<EntityUpdatedMessage> topicChanges() {
        return messagingModule.messageSenderFactory().makeMessageSender(topicChanges,
                JacksonMessageSerializer.forType(EntityUpdatedMessage.class));
    }

    @Bean
    @Lazy(true)
    public MessageSender<ScheduleUpdateMessage> scheduleChanges() {
        return messagingModule.messageSenderFactory().makeMessageSender(scheduleChanges,
                JacksonMessageSerializer.forType(ScheduleUpdateMessage.class));
    }

    private @Autowired ChannelResolver channelResolver;
    
    public @Bean ContentGroupWriter contentGroupWriter() {
        ContentGroupWriter contentGroupWriter = new MongoContentGroupWriter(db, persistenceAuditLog(), new SystemClock());
        return contentGroupWriter;
    }
    
    public @Bean ContentGroupResolver contentGroupResolver() {
        return new MongoContentGroupResolver(db);
    }
    
    public @Primary @Bean ContentWriter contentWriter() {
        ContentWriter contentWriter = new MongoContentWriter(db, lookupStore(), persistenceAuditLog(), 
                playerResolver(), serviceResolver(), new SystemClock());
        
        contentWriter = new EquivalenceWritingContentWriter(contentWriter, explicitLookupWriter());
        if (Boolean.valueOf(messagingEnabled)) {
            contentWriter = new MessageQueueingContentWriter(contentChanges(), contentWriter);
        }
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
        return TransitiveLookupWriter.explicitTransitiveLookupWriter(entryStore);
    }

    public @Bean
    LookupWriter generatedLookupWriter() {
        MongoLookupEntryStore entryStore = new MongoLookupEntryStore(db.collection("lookup"));
        return TransitiveLookupWriter.generatedTransitiveLookupWriter(entryStore);
    }

    public @Primary
    @Bean
    MongoScheduleStore scheduleStore() {
        try {
            return new MongoScheduleStore(db, channelResolver, contentResolver(), equivContentResolver(), scheduleChanges());
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
        PersonStore personStore = new MongoPersonStore(db, 
                TransitiveLookupWriter.explicitTransitiveLookupWriter(personLookupEntryStore), 
                personLookupEntryStore, 
                persistenceAuditLog());
        
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
        TopicStore store = new MongoTopicStore(db, persistenceAuditLog());
        if (Boolean.valueOf(messagingEnabled)) {
            store = new MessageQueueingTopicWriter(topicChanges(), store);
        }
        return new TopicCreatingTopicResolver(store, new MongoSequentialIdGenerator(db, "topic"));
    }

    public @Bean ServiceResolver serviceResolver() {
        return new CachingServiceResolver(new MongoServiceStore(db));
    }
    
    public @Bean PlayerResolver playerResolver() {
        return new CachingPlayerResolver(new MongoPlayerStore(db));
    }
    
    public @Primary @Bean TopicQueryResolver topicQueryResolver() {
        return new MongoTopicStore(db, persistenceAuditLog());
    }
    
    public @Primary @Bean SegmentWriter segmentWriter() {
        return new IdSettingSegmentWriter(new MongoSegmentWriter(db, new SubstitutionTableNumberCodec()), segmentResolver(), new MongoSequentialIdGenerator(db, "segment"));
    }

    public @Primary @Bean SegmentResolver segmentResolver() {
        return new MongoSegmentResolver(db, new SubstitutionTableNumberCodec());
    }
    
    public @Bean EventStore eventStore() {
        return new IdSettingEventStore(new MongoEventStore(db), new MongoSequentialIdGenerator(db, "events"));
    }
    
    // not sure if this is right
    public @Bean OrganisationStore organisationStore() {
        LookupEntryStore organisationLookupEntryStore = new MongoLookupEntryStore(db.collection("organisationLookup"));
        OrganisationStore organisationStore = new MongoOrganisationStore(db, 
                TransitiveLookupWriter.explicitTransitiveLookupWriter(organisationLookupEntryStore), 
                organisationLookupEntryStore, 
                persistenceAuditLog());
        
        if (Boolean.valueOf(generateIds)) {
            organisationStore = new IdSettingOrganisationStore(organisationStore, new MongoSequentialIdGenerator(db, "organisations"));
        }
        return organisationStore;
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
    
    public @Bean ContentPurger contentPurger() {
        return new MongoContentPurger(contentLister(), 
                contentResolver(), 
                contentWriter(), 
                new MongoContentTables(db), 
                db.collection("lookup"), 
                explicitLookupWriter(), 
                generatedLookupWriter());
    }
    
    public @Bean PersistenceAuditLog persistenceAuditLog() {
        return new PerHourAndDayMongoPersistenceAuditLog(new DatabasedMongo(mongo, auditDbName));
    }
}
