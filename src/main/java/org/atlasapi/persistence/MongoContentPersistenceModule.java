package org.atlasapi.persistence;

import org.atlasapi.persistence.content.DefaultEquivalentContentResolver;
import org.atlasapi.persistence.content.LookupResolvingContentResolver;
import org.atlasapi.persistence.content.mongo.MongoContentGroupResolver;
import org.atlasapi.persistence.content.mongo.MongoContentGroupWriter;
import org.atlasapi.persistence.content.mongo.MongoContentLister;
import org.atlasapi.persistence.content.mongo.MongoContentResolver;
import org.atlasapi.persistence.content.mongo.MongoContentWriter;
import org.atlasapi.persistence.content.mongo.MongoPersonStore;
import org.atlasapi.persistence.content.mongo.MongoProductStore;
import org.atlasapi.persistence.content.mongo.MongoTopicStore;
import org.atlasapi.persistence.content.people.QueuingItemsPeopleWriter;
import org.atlasapi.persistence.content.people.QueuingPersonWriter;
import org.atlasapi.persistence.content.schedule.mongo.MongoScheduleStore;
import org.atlasapi.persistence.ids.MongoSequentialIdGenerator;
import org.atlasapi.persistence.logging.AdapterLog;
import org.atlasapi.persistence.logging.SystemOutAdapterLog;
import org.atlasapi.persistence.lookup.mongo.MongoLookupEntryStore;
import org.atlasapi.persistence.media.channel.ChannelStore;
import org.atlasapi.persistence.media.channel.MongoChannelGroupStore;
import org.atlasapi.persistence.media.channel.MongoChannelStore;
import org.atlasapi.persistence.media.product.ProductResolver;
import org.atlasapi.persistence.media.segment.MongoSegmentResolver;
import org.atlasapi.persistence.media.segment.MongoSegmentWriter;
import org.atlasapi.persistence.messaging.mongo.MongoMessageStore;
import org.atlasapi.persistence.shorturls.MongoShortUrlSaver;
import org.atlasapi.persistence.topic.TopicCreatingTopicResolver;
import org.atlasapi.persistence.topic.TopicQueryResolver;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.persistence.mongo.health.MongoIOProbe;
import com.metabroadcast.common.time.SystemClock;
import com.mongodb.Mongo;
import com.mongodb.WriteConcern;

public class MongoContentPersistenceModule {

    private final MongoLookupEntryStore lookupStore;
    private final MongoContentGroupWriter contentGroupWriter;
    private final MongoContentGroupResolver contentGroupResolver;
    private final MongoContentWriter contentWriter;
    private final LookupResolvingContentResolver contentResolver;
    private final MongoContentResolver knownTypeContentResolver;
    private final MongoScheduleStore scheduleStore;
    private final QueuingItemsPeopleWriter itemsPeopleWriter;
    private final MongoPersonStore personStore;
    private final MongoShortUrlSaver shortUrlSaver;
    private final ChannelStore channelStore;
    private final MongoContentLister contentLister;
    private final TopicCreatingTopicResolver topicCreatingTopicResolver;
    private final MongoTopicStore topicStore;
    private final MongoSegmentWriter segmentWriter;
    private final MongoSegmentResolver segmentResolver;
    private final MongoChannelGroupStore channelGroupStore;
    private final MongoProductStore productStore;
    private final MongoMessageStore messageStore;

    private @Autowired Mongo mongo;
	private @Autowired DatabasedMongo db;
	private @Autowired AdapterLog log;

    public MongoContentPersistenceModule(DatabasedMongo db) {
        AdapterLog log = new SystemOutAdapterLog();
        this.lookupStore = new MongoLookupEntryStore(db);
        this.contentGroupWriter = new MongoContentGroupWriter(db, new SystemClock());
        this.contentGroupResolver = new MongoContentGroupResolver(db);
        this.contentWriter = new MongoContentWriter(db, lookupStore, new SystemClock());
        this.knownTypeContentResolver = new MongoContentResolver(db);
        this.contentResolver = new LookupResolvingContentResolver(knownTypeContentResolver, lookupStore);
        this.channelGroupStore = new MongoChannelGroupStore(db);
        this.channelStore = new MongoChannelStore(db, channelGroupStore, channelGroupStore);
        this.scheduleStore = new MongoScheduleStore(db, contentResolver, channelStore, new DefaultEquivalentContentResolver(knownTypeContentResolver, lookupStore));
        this.personStore = new MongoPersonStore(db);
        this.itemsPeopleWriter = new QueuingItemsPeopleWriter(new QueuingPersonWriter(personStore, log), log);
        this.shortUrlSaver = new MongoShortUrlSaver(db);
        this.contentLister = new MongoContentLister(db);
        this.topicStore = new MongoTopicStore(db);
        this.topicCreatingTopicResolver = new TopicCreatingTopicResolver(topicStore, new MongoSequentialIdGenerator(db, "topic"));
        this.segmentWriter = new MongoSegmentWriter(db, new SubstitutionTableNumberCodec());
        this.segmentResolver = new MongoSegmentResolver(db, new SubstitutionTableNumberCodec());
        this.productStore = new MongoProductStore(db);
        this.messageStore = new MongoMessageStore(db);
    }

    public MongoContentGroupWriter contentGroupWriter() {
        return contentGroupWriter;
    }

    public MongoContentGroupResolver contentGroupResolver() {
        return contentGroupResolver;
    }

    public MongoContentWriter contentWriter() {
        return contentWriter;
    }

    public LookupResolvingContentResolver contentResolver() {
        return contentResolver;
    }

    public MongoContentResolver knownTypeContentResolver() {
        return knownTypeContentResolver;
    }

    public MongoLookupEntryStore lookupStore() {
        return lookupStore;
    }

    public MongoScheduleStore scheduleStore() {
        return scheduleStore;
    }

    public QueuingItemsPeopleWriter itemsPeopleWriter() {
        return itemsPeopleWriter;
    }

    public MongoPersonStore personStore() {
        return personStore;
    }
    
    public MongoShortUrlSaver shortUrlSaver() {
        return shortUrlSaver;
    }

    public MongoContentLister contentLister() {
        return contentLister;
    }

    public TopicCreatingTopicResolver topicStore() {
        return topicCreatingTopicResolver;
    }
        
    public MongoSegmentWriter segmentWriter() {
        return segmentWriter;
    }

    public MongoSegmentResolver segmentResolver() {
        return segmentResolver;
    }
    
    public ChannelStore channelStore() {
        return channelStore;
    }

    public MongoChannelGroupStore channelGroupStore() {
        return channelGroupStore;
    }

    public MongoProductStore productStore() {
        return productStore;
    }

    public MongoMessageStore messageStore() {
        return messageStore;
	}

    public @Primary @Bean TopicQueryResolver topicQueryResolver() {
        return new MongoTopicStore(db);
    }
        
    public @Primary @Bean ProductResolver productResolver() {
        return new MongoProductStore(db);
    }

    @Bean
    MongoIOProbe mongoIoSetProbe() {
        return new MongoIOProbe(mongo).withWriteConcern(WriteConcern.REPLICAS_SAFE);
    }
}
