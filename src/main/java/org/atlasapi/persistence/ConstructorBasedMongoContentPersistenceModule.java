package org.atlasapi.persistence;

import com.google.common.base.Optional;
import com.metabroadcast.common.ids.IdGenerator;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.persistence.mongo.health.MongoIOProbe;
import com.metabroadcast.common.properties.Parameter;
import com.metabroadcast.common.queue.MessageSender;
import com.metabroadcast.common.time.SystemClock;
import com.mongodb.Mongo;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import org.atlasapi.media.channel.CachingChannelStore;
import org.atlasapi.media.channel.ChannelGroupStore;
import org.atlasapi.media.channel.ChannelStore;
import org.atlasapi.media.channel.MongoChannelGroupStore;
import org.atlasapi.media.channel.MongoChannelStore;
import org.atlasapi.media.channel.ServiceChannelStore;
import org.atlasapi.media.entity.Event;
import org.atlasapi.media.entity.Topic;
import org.atlasapi.media.product.IdSettingProductStore;
import org.atlasapi.media.product.ProductResolver;
import org.atlasapi.media.product.ProductStore;
import org.atlasapi.media.segment.IdSettingSegmentWriter;
import org.atlasapi.media.segment.MongoSegmentResolver;
import org.atlasapi.media.segment.MongoSegmentWriter;
import org.atlasapi.media.segment.SegmentResolver;
import org.atlasapi.media.segment.SegmentWriter;
import org.atlasapi.messaging.v3.ContentEquivalenceAssertionMessage;
import org.atlasapi.messaging.v3.ContentEquivalenceAssertionMessenger;
import org.atlasapi.messaging.v3.EntityUpdatedMessage;
import org.atlasapi.messaging.v3.JacksonMessageSerializer;
import org.atlasapi.messaging.v3.MessagingModule;
import org.atlasapi.messaging.v3.ScheduleUpdateMessage;
import org.atlasapi.persistence.audit.NoLoggingPersistenceAuditLog;
import org.atlasapi.persistence.audit.PerHourAndDayMongoPersistenceAuditLog;
import org.atlasapi.persistence.audit.PersistenceAuditLog;
import org.atlasapi.persistence.content.ContentGroupResolver;
import org.atlasapi.persistence.content.ContentGroupWriter;
import org.atlasapi.persistence.content.ContentPurger;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.DefaultEquivalentContentResolver;
import org.atlasapi.persistence.content.EquivalenceContentWriter;
import org.atlasapi.persistence.content.EquivalenceWritingContentWriter;
import org.atlasapi.persistence.content.EquivalentContentResolver;
import org.atlasapi.persistence.content.IdSettingContentWriter;
import org.atlasapi.persistence.content.KnownTypeContentResolver;
import org.atlasapi.persistence.content.LookupBackedContentIdGenerator;
import org.atlasapi.persistence.content.LookupResolvingContentResolver;
import org.atlasapi.persistence.content.MessageQueueingContentWriter;
import org.atlasapi.persistence.content.MessageQueueingEquivalenceContentWriter;
import org.atlasapi.persistence.content.MessageQueuingContentGroupWriter;
import org.atlasapi.persistence.content.PeopleQueryResolver;
import org.atlasapi.persistence.content.listing.MongoProgressStore;
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
import org.atlasapi.persistence.content.organisation.QueueingOrganisationStore;
import org.atlasapi.persistence.content.people.EquivalatingPeopleResolver;
import org.atlasapi.persistence.content.people.IdSettingPersonStore;
import org.atlasapi.persistence.content.people.ItemsPeopleWriter;
import org.atlasapi.persistence.content.people.PersonStore;
import org.atlasapi.persistence.content.people.QueuingItemsPeopleWriter;
import org.atlasapi.persistence.content.people.QueuingPersonWriter;
import org.atlasapi.persistence.content.schedule.mongo.MongoScheduleStore;
import org.atlasapi.persistence.event.EventResolver;
import org.atlasapi.persistence.event.EventStore;
import org.atlasapi.persistence.event.EventWriter;
import org.atlasapi.persistence.event.IdSettingEventStore;
import org.atlasapi.persistence.event.MessageQueueingEventWriter;
import org.atlasapi.persistence.event.MongoEventStore;
import org.atlasapi.persistence.ids.MongoSequentialIdGenerator;
import org.atlasapi.persistence.logging.AdapterLog;
import org.atlasapi.persistence.lookup.LookupWriter;
import org.atlasapi.persistence.lookup.NoLockTransitiveLookupWriter;
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
import org.joda.time.DateTime;
import org.springframework.context.annotation.Bean;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * This abstraction logic of the ContentPersistenceModule is done because we do not want to
 * wire Spring and our Dropwizard microservices project. Currently, BT ingesters extracted from
 * monolith will be using atlas-persistence to write to mongoDB directly until we decided to
 * move it to using the Atlas POST api.
 *
 * This is intended for non-DI use.
 */
public class ConstructorBasedMongoContentPersistenceModule implements ContentPersistenceModule {

    private static final String LOOKUP = "lookup";

    private final ReadPreference readPreference;
    private final Mongo mongo;
    private final DatabasedMongo db;
    private final AdapterLog log;
    private final MessagingModule messagingModule;

    private final String contentChanges;
    private final String topicChanges;
    private final String scheduleChanges;
    private final String contentGroupChanges;
    private final String eventChanges;
    private final String organisationChanges;
    private final boolean messagingEnabled;
    private final String auditDbName;
    private final boolean auditEnabled;
    private final String equivAssertDest;

    // This decides whether to use MongoChannelStore or CachingChannelStore which has
    // additional methods.
    private final Parameter processingConfig;

    //This MongoContentPersistenceModule is intended to be used by projects without DI.
    public ConstructorBasedMongoContentPersistenceModule(
            Mongo mongo,
            DatabasedMongo db,
            MessagingModule messagingModule,
            String auditDbName,
            AdapterLog log,
            ReadPreference readPreference,
            String contentChanges,
            String topicChanges,
            String scheduleChanges,
            String contentGroupChanges,
            String eventChanges,
            String organisationChanges,
            boolean messagingEnabled,
            boolean auditEnabled,
            Parameter processingConfig,
            String equivAssertDest
    ) {
        this.mongo = checkNotNull(mongo);
        this.db = checkNotNull(db);
        this.log = checkNotNull(log);
        this.messagingModule = checkNotNull(messagingModule);
        this.auditDbName = checkNotNull(auditDbName);
        this.readPreference = checkNotNull(readPreference);

        this.contentChanges = checkNotNull(contentChanges);
        this.topicChanges = checkNotNull(topicChanges);
        this.scheduleChanges = checkNotNull(scheduleChanges);
        this.contentGroupChanges = checkNotNull(contentGroupChanges);
        this.eventChanges = checkNotNull(eventChanges);
        this.organisationChanges = checkNotNull(organisationChanges);
        this.messagingEnabled = messagingEnabled;
        this.auditEnabled = checkNotNull(auditEnabled);
        this.processingConfig = checkNotNull(processingConfig);
        this.equivAssertDest = checkNotNull(equivAssertDest);
    }

    public MessageSender<EntityUpdatedMessage> contentChanges() {
        return messagingModule.messageSenderFactory().makeMessageSender(contentChanges,
                JacksonMessageSerializer.forType(EntityUpdatedMessage.class));
    }

    public MessageSender<EntityUpdatedMessage> topicChanges() {
        return messagingModule.messageSenderFactory().makeMessageSender(topicChanges,
                JacksonMessageSerializer.forType(EntityUpdatedMessage.class));
    }

    public MessageSender<ScheduleUpdateMessage> scheduleChanges() {
        return messagingModule.messageSenderFactory().makeMessageSender(scheduleChanges,
                JacksonMessageSerializer.forType(ScheduleUpdateMessage.class));
    }

    public MessageSender<EntityUpdatedMessage> eventChanges() {
        return messagingModule.messageSenderFactory().makeMessageSender(eventChanges,
                JacksonMessageSerializer.forType(EntityUpdatedMessage.class));
    }

    public MessageSender<EntityUpdatedMessage> organizationChanges() {
        return messagingModule.messageSenderFactory().makeMessageSender(organisationChanges,
                JacksonMessageSerializer.forType(EntityUpdatedMessage.class));
    }

    private ContentEquivalenceAssertionMessenger messenger() {
        return ContentEquivalenceAssertionMessenger.create(
                messagingModule.messageSenderFactory()
                        .makeMessageSender(
                                equivAssertDest,
                                JacksonMessageSerializer.forType(
                                        ContentEquivalenceAssertionMessage.class
                                )
                        ),
                new SystemClock(),
                lookupStore()
        );
    }

    @Override
    public ContentGroupWriter contentGroupWriter() {
        MessageSender<EntityUpdatedMessage> messageSender = messagingModule.messageSenderFactory()
                .makeMessageSender(
                        contentGroupChanges,
                        JacksonMessageSerializer.forType(EntityUpdatedMessage.class)
                );

        SystemClock clock = new SystemClock();

        return new MessageQueuingContentGroupWriter(
                new MongoContentGroupWriter(db, persistenceAuditLog(), clock),
                messageSender,
                clock
        );
    }

    @Override
    public ContentGroupResolver contentGroupResolver() {
        return new MongoContentGroupResolver(db);
    }

    @Override
    public ContentWriter mongoContentWriter() {
        return new MongoContentWriter(
                db, lookupStore(), persistenceAuditLog(),
                playerResolver(), serviceResolver(), new SystemClock()
        );
    }

    @Override
    public ContentWriter contentWriter() {
        ContentWriter contentWriter = new MongoContentWriter(
                db, lookupStore(), persistenceAuditLog(),
                playerResolver(), serviceResolver(), new SystemClock()
        );

        contentWriter = new EquivalenceWritingContentWriter(contentWriter, explicitLookupWriter());
        if (messagingEnabled) {
            contentWriter = new MessageQueueingContentWriter(
                    messenger(),
                    contentChanges(),
                    contentWriter,
                    contentResolver()
            );
        }

        contentWriter = new IdSettingContentWriter(
                contentWriter, lookupBackedContentIdGenerator()
        );

        return contentWriter;
    }

    @Override
    public EquivalenceContentWriter nonIdSettingContentWriter() {
        ContentWriter contentWriter = new MongoContentWriter(
                db, lookupStore(), persistenceAuditLog(),
                playerResolver(), serviceResolver(), new SystemClock()
        );

        EquivalenceContentWriter equivalenceContentWriter = new EquivalenceWritingContentWriter(contentWriter, explicitLookupWriter());
        if (messagingEnabled) {
            equivalenceContentWriter = new MessageQueueingEquivalenceContentWriter(
                    messenger(),
                    contentChanges(),
                    equivalenceContentWriter,
                    contentResolver()
            );
        }

        return equivalenceContentWriter;
    }

    @Override
    public ContentWriter nonIdNoLockSettingContentWriter() {
        ContentWriter contentWriter = new MongoContentWriter(
                db, lookupStore(), persistenceAuditLog(),
                playerResolver(), serviceResolver(), new SystemClock()
        );

        contentWriter = new EquivalenceWritingContentWriter(contentWriter, explicitNoLockLookupWriter());
        if (messagingEnabled) {
            contentWriter = new MessageQueueingContentWriter(
                    messenger(),
                    contentChanges(),
                    contentWriter,
                    contentResolver()
            );
        }

        return contentWriter;
    }

    @Override
    public ContentResolver contentResolver() {
        return new LookupResolvingContentResolver(knownTypeContentResolver(), lookupStore());
    }

    public KnownTypeContentResolver knownTypeContentResolver() {
        return new MongoContentResolver(db, lookupStore());
    }

    public MongoLookupEntryStore lookupStore() {
        return new MongoLookupEntryStore(db.collection(LOOKUP),
                persistenceAuditLog(), readPreference);
    }

    @Override
    public LookupBackedContentIdGenerator lookupBackedContentIdGenerator() {
        return new LookupBackedContentIdGenerator(lookupStore(), contentIdGenerator());
    }

    @Override
    public IdGenerator contentIdGenerator() {
        return new MongoSequentialIdGenerator(db, "content");
    }

    protected LookupWriter explicitLookupWriter() {
        MongoLookupEntryStore entryStore = new MongoLookupEntryStore(db.collection("lookup"),
                persistenceAuditLog(), ReadPreference.primary());
        return TransitiveLookupWriter.explicitTransitiveLookupWriter(entryStore);
    }

    protected LookupWriter explicitNoLockLookupWriter() {
        MongoLookupEntryStore entryStore = new MongoLookupEntryStore(db.collection("lookup"),
                persistenceAuditLog(), ReadPreference.primary());
        return NoLockTransitiveLookupWriter.explicitTransitiveLookupWriter(entryStore);
    }

    public LookupWriter generatedLookupWriter() {
        MongoLookupEntryStore entryStore = new MongoLookupEntryStore(db.collection("lookup"),
                persistenceAuditLog(), ReadPreference.primary());
        return TransitiveLookupWriter.generatedTransitiveLookupWriter(entryStore);
    }

    /**
     * We are passing in channel store here instead of initializing it like the other arguements
     * is because the start() and stop() of CachingChannelStore has to be called pre-construction
     * and post-destruction.
     * So, we would like to use the singleton ChannelStore bean initialized in the
     * Spring MongoContentPersistenceModule.
     */
    public MongoScheduleStore scheduleStore(ChannelStore channelStore) {
        try {
            return new MongoScheduleStore(
                    db,
                    channelStore,
                    contentResolver(),
                    mongoContentWriter(),
                    equivContentResolver(),
                    scheduleChanges()
            );
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public EquivalentContentResolver equivContentResolver() {
        return new DefaultEquivalentContentResolver(knownTypeContentResolver(), lookupStore());
    }

    @Override
    public ItemsPeopleWriter itemsPeopleWriter() {
        return new QueuingItemsPeopleWriter(personWriter(), log);
    }

    public QueuingPersonWriter personWriter() {
        return new QueuingPersonWriter(personStore(), log);
    }

    public PersonStore personStore() {
        LookupEntryStore personLookupEntryStore = new MongoLookupEntryStore(
                db.collection("peopleLookup"),
                persistenceAuditLog(),
                readPreference
        );
        PersonStore personStore = new MongoPersonStore(db,
                TransitiveLookupWriter.explicitTransitiveLookupWriter(personLookupEntryStore),
                personLookupEntryStore,
                persistenceAuditLog());

        //For now people occupy the same id space as content.
        personStore = new IdSettingPersonStore(
                personStore, new MongoSequentialIdGenerator(db, "content")
        );

        return personStore;
    }

    @Override
    public ShortUrlSaver shortUrlSaver() {
        return new MongoShortUrlSaver(db);
    }

    public MongoContentLister contentLister() {
        return new MongoContentLister(db, knownTypeContentResolver());
    }

    @Override
    public TopicStore topicStore() {
        TopicStore store = new MongoTopicStore(db, persistenceAuditLog());
        if (messagingEnabled) {
            store = new MessageQueueingTopicWriter(topicChanges(), store);
        }
        return new TopicCreatingTopicResolver(store, new MongoSequentialIdGenerator(db, "topic"));
    }

    public ServiceResolver serviceResolver() {
        return new CachingServiceResolver(new MongoServiceStore(db));
    }

    public PlayerResolver playerResolver() {
        return new CachingPlayerResolver(new MongoPlayerStore(db));
    }

    @Override
    public TopicQueryResolver topicQueryResolver() {
        return new MongoTopicStore(db, persistenceAuditLog());
    }

    @Override
    public SegmentWriter segmentWriter() {
        return new IdSettingSegmentWriter(
                new MongoSegmentWriter(db, new SubstitutionTableNumberCodec()),
                segmentResolver(),
                new MongoSequentialIdGenerator(db, "segment")
        );
    }

    @Override
    public SegmentResolver segmentResolver() {
        return new MongoSegmentResolver(db, new SubstitutionTableNumberCodec());
    }

    /**
     * @deprecated Use {@link MongoContentPersistenceModule#eventWriter()} and
     * {@link MongoContentPersistenceModule#eventResolver()} instead
     */
    @Deprecated
    protected EventStore eventStore() {
        return new EventStore() {

            private EventWriter eventWriter = eventWriter();
            private EventResolver eventResolver = eventResolver();

            @Override public Optional<Event> fetch(Long id) {
                return eventResolver.fetch(id);
            }

            @Override public Optional<Event> fetch(String uri) {
                return eventResolver.fetch(uri);
            }

            @Override public Iterable<Event> fetch(Optional<Topic> eventGroup,
                    Optional<DateTime> from) {
                return eventResolver.fetch(eventGroup, from);
            }

            @Override public Event createOrUpdate(Event event) {
                return eventWriter.createOrUpdate(event);
            }
        };
    }

    public EventWriter eventWriter() {
        IdSettingEventStore eventStore = new IdSettingEventStore(new MongoEventStore(db),
                new MongoSequentialIdGenerator(db, "events"));

        if(messagingEnabled) {
            return new MessageQueueingEventWriter(eventStore, eventChanges());
        }
        return eventStore;
    }
    @Bean
    public EventResolver eventResolver() {
        return new MongoEventStore(db);
    }

    public OrganisationStore organisationStore() {

        LookupEntryStore organisationLookupEntryStore = new MongoLookupEntryStore(
                db.collection("organisationLookup"),
                persistenceAuditLog(),
                readPreference
        );
        OrganisationStore organisationStore = new MongoOrganisationStore(db,
                TransitiveLookupWriter.explicitTransitiveLookupWriter(organisationLookupEntryStore),
                organisationLookupEntryStore,
                persistenceAuditLog());

        organisationStore = new IdSettingOrganisationStore(
                organisationStore, new MongoSequentialIdGenerator(db, "organisations")
        );

        if (messagingEnabled) {
            organisationStore = new QueueingOrganisationStore(
                    organizationChanges(),
                    organisationStore
            );
        }

        return organisationStore;
    }

    public ServiceChannelStore channelStore() {

        if (processingConfig == null || !processingConfig.toBoolean()) {
            return new CachingChannelStore(
                    new MongoChannelStore(db, channelGroupStore(), channelGroupStore())
            );
        }
        return new MongoChannelStore(db, channelGroupStore(), channelGroupStore());
    }

    public ChannelGroupStore channelGroupStore() {
        return new MongoChannelGroupStore(db);
    }

    @Override
    public ProductStore productStore() {
        return new IdSettingProductStore(
                (ProductStore)productResolver(),
                new MongoSequentialIdGenerator(db, "product")
        );
    }

    @Override
    public ProductResolver productResolver() {
        return new MongoProductStore(db);
    }

    MongoIOProbe mongoIoSetProbe() {
        return new MongoIOProbe(mongo).withWriteConcern(WriteConcern.REPLICAS_SAFE);
    }

    @Override
    public PeopleQueryResolver peopleQueryResolver() {
        return new EquivalatingPeopleResolver(
                personStore(),
                new MongoLookupEntryStore(
                        db.collection("peopleLookup"),
                        persistenceAuditLog(),
                        readPreference
                )
        );
    }

    public ContentPurger contentPurger() {

        return new MongoContentPurger(contentLister(),
                contentResolver(),
                contentWriter(),
                new MongoContentTables(db),
                db.collection("lookup"),
                explicitLookupWriter(),
                generatedLookupWriter());
    }

    public PersistenceAuditLog persistenceAuditLog() {

        if (auditEnabled) {
            return new PerHourAndDayMongoPersistenceAuditLog(
                    new DatabasedMongo(mongo, auditDbName)
            );
        } else {
            return new NoLoggingPersistenceAuditLog();
        }
    }

    public MongoProgressStore mongoProgressStore() {
        return new MongoProgressStore(db);
    }
}
