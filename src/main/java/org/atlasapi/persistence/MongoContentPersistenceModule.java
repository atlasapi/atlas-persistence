package org.atlasapi.persistence;

import org.atlasapi.media.channel.ChannelGroupStore;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.channel.ChannelStore;
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Primary;

import com.google.common.annotations.VisibleForTesting;
import com.metabroadcast.common.ids.IdGenerator;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.persistence.mongo.health.MongoIOProbe;
import com.metabroadcast.common.properties.Configurer;
import com.metabroadcast.common.properties.Parameter;
import com.metabroadcast.common.queue.MessageSender;
import com.mongodb.Mongo;
import com.mongodb.ReadPreference;

/**
 * This is the Spring version of the MongoContentPersistenceModule.
 * The non-DI version serves as a delegate in this class.
 */
@Configuration
@Import(KafkaMessagingModule.class)
public class MongoContentPersistenceModule {

    private @Autowired ReadPreference readPreference;
    private @Autowired Mongo mongo;
    private @Autowired DatabasedMongo db;
    private @Autowired AdapterLog log;
    private @Autowired MessagingModule messagingModule;
    
    private final Parameter processingConfig = Configurer.get("processing.config");
    
    private @Value("${messaging.destination.content.changes}") String contentChanges;
    private @Value("${messaging.destination.topics.changes}") String topicChanges;
    private @Value("${messaging.destination.schedule.changes}") String scheduleChanges;
    private @Value("${messaging.destination.content.group.changes}") String contentGroupChanges;
    private @Value("${messaging.destination.event.changes}") String eventChanges;
    private @Value("${messaging.destination.organisation.changes}") String organisationChanges;
    private @Value("${ids.generate}") String generateIds;
    private @Value("${messaging.enabled}") String messagingEnabled;
    private @Value("${mongo.audit.dbname}") String auditDbName;
    private @Value("${mongo.audit.enabled}") boolean auditEnabled;

    private ConstructorBasedMongoContentPersistenceModule persistenceModule;

    @VisibleForTesting
    public MongoContentPersistenceModule(Mongo mongo, DatabasedMongo db, MessagingModule messagingModule, String auditDbName, AdapterLog log,
            ReadPreference readPreference) {
        this.mongo = mongo;
        this.db = db;
        this.log = log;
        this.messagingModule = messagingModule;
        this.generateIds = "true";
        this.auditDbName = auditDbName;
        this.readPreference = readPreference;
    }

    @Bean
    public ConstructorBasedMongoContentPersistenceModule persistenceModule() {
        return persistenceModule = new ConstructorBasedMongoContentPersistenceModule(
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
                generateIds,
                messagingEnabled,
                auditEnabled,
                processingConfig

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

    private @Autowired ChannelResolver channelResolver;

    public @Bean ContentGroupWriter contentGroupWriter() {
        return persistenceModule().contentGroupWriter();
    }

    public @Bean ContentGroupResolver contentGroupResolver() {
        return persistenceModule().contentGroupResolver();
    }

    public @Primary @Bean ContentWriter contentWriter() {

        return persistenceModule().contentWriter();
    }

    public @Primary @Bean ContentResolver contentResolver() {
        return persistenceModule().contentResolver();
    }

    public @Primary @Bean KnownTypeContentResolver knownTypeContentResolver() {
        return persistenceModule().knownTypeContentResolver();
    }

    public @Primary @Bean MongoLookupEntryStore lookupStore() {
        return persistenceModule().lookupStore();
    }

    public @Primary @Bean IdGenerator contentIdGenerator() {
        return persistenceModule().contentIdGenerator();
    }

    public LookupWriter explicitLookupWriter() {

        return persistenceModule().explicitLookupWriter();
    }

    public @Bean
    LookupWriter generatedLookupWriter() {
        return persistenceModule().generatedLookupWriter();
    }

    public @Primary
    @Bean
    MongoScheduleStore scheduleStore() {

        return persistenceModule().scheduleStore();
    }

    public @Primary @Bean EquivalentContentResolver equivContentResolver() {
        return persistenceModule().equivContentResolver();
    }

    public @Primary @Bean ItemsPeopleWriter itemsPeopleWriter() {
        return persistenceModule().itemsPeopleWriter();
    }

    public @Primary @Bean QueuingPersonWriter personWriter() {
        return persistenceModule().personWriter();
    }

    public @Primary @Bean PersonStore personStore() {

        return persistenceModule().personStore();
    }

    public @Primary @Bean ShortUrlSaver shortUrlSaver() {
        return persistenceModule().shortUrlSaver();
    }

    public @Primary @Bean MongoContentLister contentLister() {
        return persistenceModule().contentLister();
    }

    public @Primary @Bean TopicStore topicStore() {

        return persistenceModule().topicStore();
    }

    public @Bean ServiceResolver serviceResolver() {

        return persistenceModule().serviceResolver();
    }

    public @Bean PlayerResolver playerResolver() {

        return persistenceModule().playerResolver();
    }

    public @Primary @Bean TopicQueryResolver topicQueryResolver() {
        return persistenceModule().topicQueryResolver();
    }

    public @Primary @Bean SegmentWriter segmentWriter() {
        return persistenceModule().segmentWriter();
    }

    public @Primary @Bean SegmentResolver segmentResolver() {
        return persistenceModule().segmentResolver();
    }

    /**
     * @deprecated Use {@link MongoContentPersistenceModule#eventWriter()} and
     * {@link MongoContentPersistenceModule#eventResolver()} instead
     */
    @Deprecated
    public @Bean EventStore eventStore() {
        return persistenceModule().eventStore();
    }

    public @Bean EventWriter eventWriter() {

        return persistenceModule().eventWriter();
    }

    public @Bean EventResolver eventResolver() {

        return persistenceModule().eventResolver();
    }

    // not sure if this is right
    public @Bean OrganisationStore organisationStore() {
        return persistenceModule().organisationStore();
    }

    public @Primary @Bean ChannelStore channelStore() {

        return persistenceModule().channelStore();
    }

    public @Primary @Bean ChannelGroupStore channelGroupStore() {
        return persistenceModule().channelGroupStore();
    }

    public @Primary @Bean ProductStore productStore() {

        return persistenceModule().productStore();
    }

    public @Primary @Bean ProductResolver productResolver() {

        return persistenceModule().productResolver();
    }

    @Bean
    MongoIOProbe mongoIoSetProbe() {

        return persistenceModule().mongoIoSetProbe();
    }

    public @Bean PeopleQueryResolver peopleQueryResolver() {

        return persistenceModule().peopleQueryResolver();
    }

    public @Bean ContentPurger contentPurger() {

        return persistenceModule().contentPurger();
    }

    public @Bean PersistenceAuditLog persistenceAuditLog() {

        return persistenceModule().persistenceAuditLog();
    }

    public @Bean MongoProgressStore mongoProgressStore() {

        return persistenceModule().mongoProgressStore();
    }
}
