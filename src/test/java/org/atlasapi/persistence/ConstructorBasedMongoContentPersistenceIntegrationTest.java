package org.atlasapi.persistence;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

import java.util.Map;

import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelGroup;
import org.atlasapi.media.channel.ChannelGroupStore;
import org.atlasapi.media.channel.ChannelStore;
import org.atlasapi.media.channel.Platform;
import org.atlasapi.media.channel.ServiceChannelStore;
import org.atlasapi.media.entity.ChildRef;
import org.atlasapi.media.entity.ContentGroup;
import org.atlasapi.media.entity.EntityType;
import org.atlasapi.media.entity.Event;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.Organisation;
import org.atlasapi.media.entity.Person;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Topic;
import org.atlasapi.media.product.Product;
import org.atlasapi.media.product.ProductStore;
import org.atlasapi.media.segment.Segment;
import org.atlasapi.media.segment.SegmentRef;
import org.atlasapi.media.segment.SegmentResolver;
import org.atlasapi.media.segment.SegmentWriter;
import org.atlasapi.messaging.v3.MessagingModule;
import org.atlasapi.persistence.content.ContentGroupResolver;
import org.atlasapi.persistence.content.ContentGroupWriter;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.KnownTypeContentResolver;
import org.atlasapi.persistence.content.organisation.OrganisationStore;
import org.atlasapi.persistence.content.people.PersonStore;
import org.atlasapi.persistence.event.EventResolver;
import org.atlasapi.persistence.event.EventWriter;
import org.atlasapi.persistence.logging.MongoLoggingAdapter;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.atlasapi.persistence.topic.TopicStore;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.persistence.MongoTestHelper;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.properties.Parameter;
import com.metabroadcast.common.queue.Message;
import com.metabroadcast.common.queue.MessageConsumerFactory;
import com.metabroadcast.common.queue.MessageSender;
import com.metabroadcast.common.queue.MessageSenderFactory;
import com.metabroadcast.common.queue.MessageSerializer;
import com.metabroadcast.common.queue.MessagingException;
import com.mongodb.DBCursor;
import com.mongodb.Mongo;
import com.mongodb.ReadPreference;

public class ConstructorBasedMongoContentPersistenceIntegrationTest {

    private final Mongo mongo = MongoTestHelper.anEmptyMongo();
    private final DatabasedMongo db = new DatabasedMongo(mongo, "atlas");
    private final MongoLoggingAdapter adapterLog = new MongoLoggingAdapter(db);
    private final MessagingModule messagingModule = new MessagingModule(){
        @Override
        public MessageSenderFactory messageSenderFactory() {
            return new MessageSenderFactory() {
                
                @Override
                public <M extends Message> MessageSender<M> makeMessageSender(String destination,
                        MessageSerializer<? super M> serializer) {
                    return new MessageSender<M>() {

                        @Override
                        public void close() throws Exception {
                        }

                        @Override
                        public void sendMessage(M message)
                                throws MessagingException {
                        }

                        @Override
                        public void sendMessage(M m, byte[] bytes)
                                throws MessagingException {

                        }
                    };
                }
            };
        }

        @Override
        public MessageConsumerFactory<?> messageConsumerFactory() {
            return null;
        }
    };

    private ConstructorBasedMongoContentPersistenceModule module;

    @Before
    public void setUp() throws Exception {
        module = new ConstructorBasedMongoContentPersistenceModule(
                mongo,
                db,
                messagingModule,
                "atlas-audit",
                adapterLog,
                ReadPreference.PRIMARY,
                "ContentChanges",
                "TopicChanges",
                "ScheduleChanges",
                "ContentGroupChanges",
                "EventChanges",
                "OrganisationChanges",
                "true",
                "true",
                true,
                Parameter.valueOf("true")
        );

    }

    @Test
    public void testContentWritingAndRetrievalWithContentResolver() {
        
        ContentWriter contentWriter = module.contentWriter();
        ContentResolver contentResolver = module.contentResolver();
        LookupEntryStore lookupStore = module.lookupStore();

        String uri = "itemUri";
        Item item = new Item(uri, "itemCurie", Publisher.BBC);
        item.setTitle("I am a title");
        
        contentWriter.createOrUpdate(item);

        contentWriter.createOrUpdate(item);
        Iterable<LookupEntry> entries = lookupStore.entriesForCanonicalUris(ImmutableSet.of(item.getCanonicalUri()));
        LookupEntry entry = Iterables.getOnlyElement(entries);
        Long id = entry.id();
        assertThat(id, is(not(nullValue())));
        assertThat(id, is(item.getId()));

        Maybe<Identified> moreContent = contentResolver.findByCanonicalUris(ImmutableSet.of(uri)).get(uri);
        assertThat((Item) moreContent.requireValue(), is(equalTo(item)));
        assertThat(((Item) moreContent.requireValue()).getId(), is(equalTo(id)));
        assertThat(((Item) moreContent.requireValue()).getTitle(), is(equalTo(item.getTitle())));
        
        String newTitle = "Changed title";
        item.setTitle(newTitle);
        item.setId(1234L);
        
        contentWriter.createOrUpdate(item);
        
        moreContent = contentResolver.findByCanonicalUris(ImmutableSet.of(uri)).get(uri);
        assertThat((Item) moreContent.requireValue(), is(equalTo(item)));
        //ID setting is currently disabled, so this check is not valid
        //assertThat(((Item) moreContent.requireValue()).getStringId(), is(equalTo(id)));
        assertThat(((Item) moreContent.requireValue()).getTitle(), is(equalTo(newTitle)));
        
    }

    @Test
    public void testContentWritingAndRetrievalKnownTypeContentResolver() {
        ContentWriter contentWriter = module.contentWriter();
        LookupEntryStore lookupStore = module.lookupStore();
        KnownTypeContentResolver knownTypeContentResolver = module.knownTypeContentResolver();

        String uri = "itemUri";
        Item item = new Item(uri, "itemCurie", Publisher.BBC);
        item.setTitle("I am a title");

        contentWriter.createOrUpdate(item);
        Iterable<LookupEntry> entries = lookupStore.entriesForCanonicalUris(ImmutableSet.of(item.getCanonicalUri()));
        LookupEntry entry = Iterables.getOnlyElement(entries);
        Long id = entry.id();
        assertThat(id, is(not(nullValue())));
        assertThat(id, is(item.getId()));

        Maybe<Identified> maybeContent = knownTypeContentResolver.findByLookupRefs(ImmutableSet.of(entry.lookupRef())).get(uri);
        assertThat((Item) maybeContent.requireValue(), is(equalTo(item)));
        assertThat(((Item) maybeContent.requireValue()).getId(), is(equalTo(id)));
        assertThat(((Item) maybeContent.requireValue()).getTitle(), is(equalTo(item.getTitle())));
    }

    @Test
    public void testContentGroupWritingAndRetrieval() {

        ContentGroupWriter contentGroupWriter = module.contentGroupWriter();
        ContentGroupResolver contentGroupResolver = module.contentGroupResolver();

        String uri = "itemUri";

        ContentGroup contentGroup = new ContentGroup(uri, Publisher.BBC);

        contentGroupWriter.createOrUpdate(contentGroup);

        Maybe<Identified> moreContentGroup = contentGroupResolver.findByCanonicalUris(ImmutableSet.of(uri)).get(uri);
        assertThat((ContentGroup) moreContentGroup.requireValue(), is(equalTo(contentGroup)));
        assertThat(((ContentGroup) moreContentGroup.requireValue()).getPublisher(), is(equalTo(Publisher.BBC)));

    }

    @Test
    public void testEventWritingAndRetrieval() {
        EventWriter eventWriter = module.eventWriter();
        EventResolver eventResolver = module.eventResolver();

        Topic topic = new Topic(100l, "namespace", "value");
        topic.setCanonicalUri("uri");
        topic.setCurie("curie");
        topic.setPublisher(Publisher.METABROADCAST);
        Iterable<Topic> eventGroups = ImmutableSet.of(topic);

        Person person = new Person("uri", "itemCurie", Publisher.METABROADCAST);
        Iterable<Person> members = ImmutableSet.of(person);

        Organisation organisation = new Organisation(members, ImmutableSet.of("altTitle"));

        Item item = new Item("uri", "itemCurie", Publisher.METABROADCAST);
        item.setTitle("I am a title");

        ChildRef childRef = new ChildRef(100l, "uri", "sortKey", DateTime.now(), EntityType.ITEM);

        Event event = Event.builder()
                .withTitle("title")
                .withPublisher(Publisher.METABROADCAST)
                .withVenue(topic)
                .withStartTime(DateTime.now().minusHours(1))
                .withEndTime(DateTime.now())
                .withParticipants(ImmutableSet.of(person))
                .withOrganisations(ImmutableSet.of(organisation))
                .withEventGroups(eventGroups)
                .withContent(ImmutableSet.of(childRef))
                .build()
                ;
        event.setCanonicalUri("uri");

        eventWriter.createOrUpdate(event);

        Optional<Event> moreEvent = eventResolver.fetch("uri");
        assertThat((Event) moreEvent.get(), is(equalTo(event)));
        assertThat((Event) moreEvent.get(), is(equalTo(event)));

    }

    @Test
    public void testSegmentWritingAndRetrieval() {

        SegmentWriter segmentWriter = module.segmentWriter();
        SegmentResolver segmentResolver = module.segmentResolver();

        Segment segment = new Segment();
        segment.setPublisher(Publisher.BBC);
        segment.setCanonicalUri("uri");
        segment.setTitle("title");
        segment.setCurie("itemCurie");;

        segmentWriter.write(segment);

        SegmentRef segmentRef = new SegmentRef(103823l);
        Map<SegmentRef, Maybe<Segment>> segmentMap = segmentResolver.resolveById(ImmutableList.of(segmentRef));

        Maybe<Segment> maybeSegment = segmentMap.get(segmentRef);

        assertThat((Segment) maybeSegment.requireValue(), is(equalTo(segment)));
    }

    @Test
    public void testOrganisationWritingAndRetrieval() {
        OrganisationStore organisationStore = module.organisationStore();

        Person person = new Person("uri", "itemCurie", Publisher.METABROADCAST);
        Iterable<Person> members = ImmutableSet.of(person);

        Organisation organisation = new Organisation(members, ImmutableSet.of("altTitle"));
        organisation.setCanonicalUri("uri");
        organisation.setId(100l);
        organisation.setPublisher(Publisher.METABROADCAST);

        organisationStore.createOrUpdateOrganisation(organisation);

        Optional<Organisation> organisationOptional = organisationStore.organisation("uri");

        assertThat((Organisation) organisationOptional.get(), is(equalTo(organisation)));
    }

    @Test
    public void testPersonWritingAndRetrieval() {
        PersonStore personStore = module.personStore();

        Person person = new Person("uri", "itemCurie", Publisher.METABROADCAST);

        personStore.createOrUpdatePerson(person);

        Optional<Person> personOptional = personStore.person("uri");

        assertThat((Person) personOptional.get(), is(equalTo(person)));
    }

    @Test
    public void testChannelWritingAndRetrieval() {

        ChannelStore channelStore = module.channelStore();
        ServiceChannelStore serviceChannelStore = module.channelStore();

        serviceChannelStore.start();

        Channel channel = Channel.builder()
                                .withSource(Publisher.BBC)
                                .withTitle("title")
                                .withHighDefinition(false)
                                .withMediaType(MediaType.AUDIO)
                                .withUri("uri")
                                .build();


        serviceChannelStore.createOrUpdate(channel);

        Maybe<Channel> channelMaybe = channelStore.fromUri("uri");

        serviceChannelStore.shutdown();

        assertThat(channelMaybe.requireValue(), is(equalTo(channel)));

    }

    @Test
    public void testChannelGroupWritingAndRetrieval() {

        ChannelGroupStore channelGroupStore = module.channelGroupStore();

        ChannelGroup channelGroup = new Platform();

        channelGroup.setPublisher(Publisher.BBC);
        channelGroup.setCanonicalUri("uri");
        channelGroup.setCurie("itemCurie");
        channelGroupStore.createOrUpdate(channelGroup);

        Optional<ChannelGroup> channelGroupMaybe = channelGroupStore.channelGroupFor("uri");
        assertThat((ChannelGroup) channelGroupMaybe.get(), is(equalTo(channelGroup)));
    }

    @Test
    public void testProductWritingAndRetrieval() {
        ProductStore productStore = module.productStore();

        Product product = new Product();
        product.setPublisher(Publisher.BBC);
        product.setCurie("itemCurie");
        product.setCanonicalUri("uri");

        productStore.store(product);

        Optional<Product> productOptional = productStore.productForSourceIdentified(Publisher.BBC, "uri");

        assertThat((Product) productOptional.get(), is(equalTo(product)));
    }

    @Test
    public void testTopicWritingAndRetrieval() {
        TopicStore topicStore = module.topicStore();

        Topic topic = new Topic(103823l, "namespace", "value");
        topic.setCanonicalUri("uri");
        topic.setPublisher(Publisher.BBC);

        topicStore.write(topic);

        Maybe<Topic> topicMaybe = topicStore.topicFor("namespace", "value");

        assertThat((Topic) topicMaybe.requireValue(), is(equalTo(topic)));
    }

}
