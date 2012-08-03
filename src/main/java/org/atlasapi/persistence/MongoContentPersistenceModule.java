package org.atlasapi.persistence;

import org.atlasapi.media.channel.ChannelGroupStore;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.channel.MongoChannelGroupStore;
import org.atlasapi.media.channel.MongoChannelStore;
import org.atlasapi.media.product.ProductResolver;
import org.atlasapi.media.product.ProductStore;
import org.atlasapi.media.segment.MongoSegmentResolver;
import org.atlasapi.media.segment.MongoSegmentWriter;
import org.atlasapi.media.segment.SegmentResolver;
import org.atlasapi.media.segment.SegmentWriter;
import org.atlasapi.persistence.content.ContentGroupResolver;
import org.atlasapi.persistence.content.ContentGroupWriter;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.KnownTypeContentResolver;
import org.atlasapi.persistence.content.LookupResolvingContentResolver;
import org.atlasapi.persistence.content.mongo.LastUpdatedContentFinder;
import org.atlasapi.persistence.content.mongo.MongoContentGroupResolver;
import org.atlasapi.persistence.content.mongo.MongoContentGroupWriter;
import org.atlasapi.persistence.content.mongo.MongoContentLister;
import org.atlasapi.persistence.content.mongo.MongoContentResolver;
import org.atlasapi.persistence.content.mongo.MongoContentWriter;
import org.atlasapi.persistence.content.mongo.MongoPersonStore;
import org.atlasapi.persistence.content.mongo.MongoProductStore;
import org.atlasapi.persistence.content.mongo.MongoTopicStore;
import org.atlasapi.persistence.content.people.ItemsPeopleWriter;
import org.atlasapi.persistence.content.people.QueuingItemsPeopleWriter;
import org.atlasapi.persistence.content.people.QueuingPersonWriter;
import org.atlasapi.persistence.content.schedule.mongo.MongoScheduleStore;
import org.atlasapi.persistence.ids.MongoSequentialIdGenerator;
import org.atlasapi.persistence.logging.AdapterLog;
import org.atlasapi.persistence.logging.SystemOutAdapterLog;
import org.atlasapi.persistence.lookup.mongo.MongoLookupEntryStore;
import org.atlasapi.persistence.shorturls.MongoShortUrlSaver;
import org.atlasapi.persistence.shorturls.ShortUrlSaver;
import org.atlasapi.persistence.topic.TopicContentLister;
import org.atlasapi.persistence.topic.TopicCreatingTopicResolver;
import org.atlasapi.persistence.topic.TopicQueryResolver;
import org.atlasapi.persistence.topic.TopicStore;

import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.time.SystemClock;
import org.atlasapi.persistence.event.RecentChangeStore;
import org.atlasapi.persistence.event.mongo.MongoRecentChangesStore;
import org.springframework.context.annotation.Bean;

public class MongoContentPersistenceModule implements ContentPersistenceModule {

	private final MongoLookupEntryStore lookupStore;
	private final ContentGroupWriter contentGroupWriter;
	private final ContentGroupResolver contentGroupResolver;
    private final ContentWriter contentWriter;
    private final MongoContentResolver contentResolver;
    private final MongoScheduleStore scheduleStore;
    private final QueuingItemsPeopleWriter itemsPeopleWriter;
    private final MongoPersonStore personStore;
    private final MongoShortUrlSaver shortUrlSaver;
    private final MongoChannelStore channelStore;
    private final MongoContentLister contentLister;
    private final TopicCreatingTopicResolver topicCreatingTopicResolver;
    private final MongoTopicStore topicStore;
    private final MongoSegmentWriter segmentWriter;
    private final MongoSegmentResolver segmentResolver;
    private final MongoChannelGroupStore channelGroupStore;
    private final MongoProductStore productStore;
    private final MongoRecentChangesStore changesStore;

    public MongoContentPersistenceModule(DatabasedMongo db) {
        AdapterLog log = new SystemOutAdapterLog();
        this.lookupStore = new MongoLookupEntryStore(db);
        this.contentGroupWriter = new MongoContentGroupWriter(db, new SystemClock());
        this.contentGroupResolver = new MongoContentGroupResolver(db);
        this.contentWriter = new MongoContentWriter(db, lookupStore, new SystemClock());
        this.contentResolver = new MongoContentResolver(db);
        this.channelStore = new MongoChannelStore(db);
        this.scheduleStore = new MongoScheduleStore(db, contentResolver(), channelStore);
        this.itemsPeopleWriter = new QueuingItemsPeopleWriter(new QueuingPersonWriter(personStore(), log), log);
        this.personStore = new MongoPersonStore(db);
        this.shortUrlSaver = new MongoShortUrlSaver(db);
        this.contentLister = new MongoContentLister(db);
        this.topicCreatingTopicResolver = new TopicCreatingTopicResolver(new MongoTopicStore(db), new MongoSequentialIdGenerator(db, "topic"));
        this.topicStore = new MongoTopicStore(db);
        this.segmentWriter = new MongoSegmentWriter(db, new SubstitutionTableNumberCodec());
        this.segmentResolver = new MongoSegmentResolver(db, new SubstitutionTableNumberCodec());
        this.channelGroupStore = new MongoChannelGroupStore(db);
        this.productStore = new MongoProductStore(db);
        this.changesStore = new MongoRecentChangesStore(db);
    }
	
    public ContentGroupWriter contentGroupWriter() {
		return contentGroupWriter;
	}
	
	public ContentGroupResolver contentGroupResolver() {
	    return contentGroupResolver;
	}
	
	public ContentWriter contentWriter() {
		return contentWriter;
	}

	public ContentResolver contentResolver() {
	    return new LookupResolvingContentResolver(knownTypeContentResolver(), lookupStore());
	}
	
	public KnownTypeContentResolver knownTypeContentResolver() {
	    return contentResolver;
	}
	
	public MongoLookupEntryStore lookupStore() {
        return lookupStore;
	}
	
	public MongoScheduleStore scheduleResolver() {
        return scheduleStore;
	}
	
    public MongoScheduleStore scheduleWriter() {
        return scheduleStore;
    }
	
	public ItemsPeopleWriter itemsPeopleWriter() {
	    return itemsPeopleWriter;
	}
	
	public MongoPersonStore personStore() {
	    return personStore;
	}

	public ShortUrlSaver shortUrlSaver() {
		return shortUrlSaver;
	}
	
	public MongoContentLister contentLister() {
		return contentLister;
    }

    public LastUpdatedContentFinder lastUpdatedContentFinder() {
        return contentLister;
    }

    public TopicStore topicStore() {
        return topicCreatingTopicResolver;
    }

    public TopicQueryResolver topicQueryResolver() {
        return topicStore;
    }
    
    public SegmentWriter segmentWriter() {
        return segmentWriter;
    }

    public SegmentResolver segmentResolver() {
        return segmentResolver;
    }
        
    public ChannelResolver channelResolver() {
    	return channelStore;
    }
    
    public ChannelGroupStore channelGroupStore() {
        return channelGroupStore;
    }

    public ProductStore productStore() {
        return productStore;
    }

    public ProductResolver productResolver() {
        return productStore;
    }

    @Bean
    public RecentChangeStore recentChangesStore() {
        return changesStore;
    }
}
