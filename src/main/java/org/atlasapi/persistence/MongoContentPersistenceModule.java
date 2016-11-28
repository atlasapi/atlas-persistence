package org.atlasapi.persistence;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.atlasapi.media.channel.ChannelGroupStore;
import org.atlasapi.media.channel.ServiceChannelStore;
import org.atlasapi.media.product.ProductResolver;
import org.atlasapi.media.product.ProductStore;
import org.atlasapi.media.segment.SegmentResolver;
import org.atlasapi.media.segment.SegmentWriter;
import org.atlasapi.messaging.v3.EntityUpdatedMessage;
import org.atlasapi.messaging.v3.KafkaMessagingModule;
import org.atlasapi.messaging.v3.MessagingModule;
import org.atlasapi.messaging.v3.ScheduleUpdateMessage;
import org.atlasapi.persistence.audit.PersistenceAuditLog;
import org.atlasapi.persistence.content.ContentGroupResolver;
import org.atlasapi.persistence.content.ContentGroupWriter;
import org.atlasapi.persistence.content.ContentPurger;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.EquivalentContentResolver;
import org.atlasapi.persistence.content.KnownTypeContentResolver;
import org.atlasapi.persistence.content.LookupBackedContentIdGenerator;
import org.atlasapi.persistence.content.PeopleQueryResolver;
import org.atlasapi.persistence.content.listing.MongoProgressStore;
import org.atlasapi.persistence.content.mongo.MongoContentLister;
import org.atlasapi.persistence.content.organisation.OrganisationStore;
import org.atlasapi.persistence.content.people.ItemsPeopleWriter;
import org.atlasapi.persistence.content.people.PersonStore;
import org.atlasapi.persistence.content.people.QueuingPersonWriter;
import org.atlasapi.persistence.content.schedule.mongo.MongoScheduleStore;
import org.atlasapi.persistence.event.EventResolver;
import org.atlasapi.persistence.event.EventStore;
import org.atlasapi.persistence.event.EventWriter;
import org.atlasapi.persistence.logging.AdapterLog;
import org.atlasapi.persistence.lookup.LookupWriter;
import org.atlasapi.persistence.lookup.mongo.MongoLookupEntryStore;
import org.atlasapi.persistence.player.PlayerResolver;
import org.atlasapi.persistence.service.ServiceResolver;
import org.atlasapi.persistence.shorturls.ShortUrlSaver;
import org.atlasapi.persistence.topic.TopicQueryResolver;
import org.atlasapi.persistence.topic.TopicStore;

import com.metabroadcast.common.ids.IdGenerator;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.persistence.mongo.health.MongoIOProbe;
import com.metabroadcast.common.properties.Configurer;
import com.metabroadcast.common.properties.Parameter;
import com.metabroadcast.common.queue.MessageSender;

import com.google.common.annotations.VisibleForTesting;
import com.mongodb.Mongo;
import com.mongodb.ReadPreference;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Primary;

/**
 * This is the Spring version of the MongoContentPersistenceModule.
 * The non-DI version serves as a delegate in this class.
 */
@Configuration
@Import(KafkaMessagingModule.class)
public class MongoContentPersistenceModule implements ContentPersistenceModule {

    public static final String NON_ID_SETTING_CONTENT_WRITER = "nonIdSettingContentWriter";

    @Autowired private ReadPreference readPreference;
    @Autowired private Mongo mongo;
    @Autowired private DatabasedMongo db;
    @Autowired private AdapterLog log;
    @Autowired private MessagingModule messagingModule;

    private final Parameter processingConfig = Configurer.get("processing.config");

    @Value("${messaging.destination.content.changes}") private String contentChanges;
    @Value("${messaging.destination.topics.changes}") private String topicChanges;
    @Value("${messaging.destination.schedule.changes}") private String scheduleChanges;
    @Value("${messaging.destination.content.group.changes}") private String contentGroupChanges;
    @Value("${messaging.destination.event.changes}") private String eventChanges;
    @Value("${messaging.destination.organisation.changes}") private String organisationChanges;
    @Value("${messaging.destination.equiv.assert}") private String equivAssertDest;
    @Value("${messaging.enabled}") private String messagingEnabled;
    @Value("${mongo.audit.dbname}") private String auditDbName;
    @Value("${mongo.audit.enabled}") private boolean auditEnabled;

    public MongoContentPersistenceModule() {}

    @VisibleForTesting
    public MongoContentPersistenceModule(
            Mongo mongo,
            DatabasedMongo db,
            MessagingModule messagingModule,
            String auditDbName,
            AdapterLog log,
            ReadPreference readPreference) {
        this.mongo = mongo;
        this.db = db;
        this.log = log;
        this.messagingModule = messagingModule;
        this.auditDbName = auditDbName;
        this.readPreference = readPreference;
    }


    /**
     * We need both this methods here to initialized the cachedValue of BackgroundComputingValue
     * that will be used by the CachingChannelStore.
     */
    @PostConstruct
    public void setUp() {
        channelStore().start();
    }

    @PreDestroy
    public void tearDown() {
        channelStore().shutdown();
    }

    public ConstructorBasedMongoContentPersistenceModule persistenceModule() {
        return new ConstructorBasedMongoContentPersistenceModule(
                mongo,
                db,
                messagingModule,
                auditDbName,
                log,
                readPreference,
                contentChanges,
                topicChanges,
                scheduleChanges,
                contentGroupChanges,
                eventChanges,
                organisationChanges,
                Boolean.valueOf(messagingEnabled),
                auditEnabled,
                processingConfig,
                equivAssertDest
        );
    }

    @Bean
    @Lazy(true)
    public MessageSender<EntityUpdatedMessage> contentChanges() {
        return persistenceModule().contentChanges();
    }

    @Bean
    @Lazy(true)
    public MessageSender<EntityUpdatedMessage> topicChanges() {
        return persistenceModule().topicChanges();
    }

    @Bean
    @Lazy(true)
    public MessageSender<ScheduleUpdateMessage> scheduleChanges() {
        return persistenceModule().scheduleChanges();
    }

    @Bean
    @Lazy(true)
    public MessageSender<EntityUpdatedMessage> eventChanges() {
        return persistenceModule().eventChanges();
    }

    @Bean
    @Lazy(true)
    public MessageSender<EntityUpdatedMessage> organizationChanges() {
        return persistenceModule().organizationChanges();
    }


    @Override
    @Bean
    public ContentGroupWriter contentGroupWriter() {
        return persistenceModule().contentGroupWriter();
    }

    @Override
    @Bean
    public ContentGroupResolver contentGroupResolver() {
        return persistenceModule().contentGroupResolver();
    }

    @Override
    @Primary
    @Bean
    public ContentWriter contentWriter() {
        return persistenceModule().contentWriter();
    }

    @Override
    @Bean(name = NON_ID_SETTING_CONTENT_WRITER)
    public ContentWriter nonIdSettingContentWriter() {
        return persistenceModule().nonIdSettingContentWriter();
    }

    @Override
    @Primary
    @Bean
    public ContentResolver contentResolver() {
        return persistenceModule().contentResolver();
    }

    @Primary
    @Bean
    public KnownTypeContentResolver knownTypeContentResolver() {
        return persistenceModule().knownTypeContentResolver();
    }

    @Primary
    @Bean
    public MongoLookupEntryStore lookupStore() {
        return persistenceModule().lookupStore();
    }

    @Override
    @Primary
    @Bean
    public IdGenerator contentIdGenerator() {
        return persistenceModule().contentIdGenerator();
    }

    @Override
    @Bean
    public LookupBackedContentIdGenerator lookupBackedContentIdGenerator() {
        return persistenceModule().lookupBackedContentIdGenerator();
    }

    public LookupWriter explicitLookupWriter() {
        return persistenceModule().explicitLookupWriter();
    }

    @Bean
    public LookupWriter generatedLookupWriter() {
        return persistenceModule().generatedLookupWriter();
    }

    @Primary
    @Bean
    public MongoScheduleStore scheduleStore() {
        return persistenceModule().scheduleStore(channelStore());
    }

    @Primary
    @Bean
    public EquivalentContentResolver equivContentResolver() {
        return persistenceModule().equivContentResolver();
    }

    @Override
    @Primary
    @Bean
    public ItemsPeopleWriter itemsPeopleWriter() {
        return persistenceModule().itemsPeopleWriter();
    }

    @Primary
    @Bean
    public QueuingPersonWriter personWriter() {
        return persistenceModule().personWriter();
    }

    @Primary
    @Bean
    public PersonStore personStore() {
        return persistenceModule().personStore();
    }

    @Override
    @Primary
    @Bean
    public ShortUrlSaver shortUrlSaver() {
        return persistenceModule().shortUrlSaver();
    }

    @Primary
    @Bean
    public MongoContentLister contentLister() {
        return persistenceModule().contentLister();
    }

    @Override
    @Primary
    @Bean
    public TopicStore topicStore() {
        return persistenceModule().topicStore();
    }

    @Bean
    public ServiceResolver serviceResolver() {
        return persistenceModule().serviceResolver();
    }

    @Bean
    public PlayerResolver playerResolver() {
        return persistenceModule().playerResolver();
    }

    @Override
    @Primary
    @Bean
    public TopicQueryResolver topicQueryResolver() {
        return persistenceModule().topicQueryResolver();
    }

    @Override
    @Primary
    @Bean
    public SegmentWriter segmentWriter() {
        return persistenceModule().segmentWriter();
    }

    @Override
    @Primary
    @Bean
    public SegmentResolver segmentResolver() {
        return persistenceModule().segmentResolver();
    }

    /**
     * @deprecated Use {@link MongoContentPersistenceModule#eventWriter()} and
     * {@link MongoContentPersistenceModule#eventResolver()} instead
     */
    @Deprecated
    @Bean
    public EventStore eventStore() {
        return persistenceModule().eventStore();
    }

    @Bean
    public EventWriter eventWriter() {
        return persistenceModule().eventWriter();
    }

    @Bean
    public EventResolver eventResolver() {
        return persistenceModule().eventResolver();
    }

    // not sure if this is right
    @Bean
    public OrganisationStore organisationStore() {
        return persistenceModule().organisationStore();
    }

    @Primary
    @Bean
    public ServiceChannelStore channelStore() {
        return persistenceModule().channelStore();
    }

    @Primary
    @Bean
    public ChannelGroupStore channelGroupStore() {
        return persistenceModule().channelGroupStore();
    }

    @Override
    @Primary
    @Bean
    public ProductStore productStore() {

        return persistenceModule().productStore();
    }

    @Override
    @Primary
    @Bean
    public ProductResolver productResolver() {
        return persistenceModule().productResolver();
    }

    @Bean
    MongoIOProbe mongoIoSetProbe() {
        return persistenceModule().mongoIoSetProbe();
    }

    @Override
    @Bean
    public PeopleQueryResolver peopleQueryResolver() {
        return persistenceModule().peopleQueryResolver();
    }

    @Bean
    public ContentPurger contentPurger() {
        return persistenceModule().contentPurger();
    }

    @Bean
    public PersistenceAuditLog persistenceAuditLog() {
        return persistenceModule().persistenceAuditLog();
    }

    @Bean
    public MongoProgressStore mongoProgressStore() {
        return persistenceModule().mongoProgressStore();
    }
}
