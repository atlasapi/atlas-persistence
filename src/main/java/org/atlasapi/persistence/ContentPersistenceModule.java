package org.atlasapi.persistence;

import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.product.ProductResolver;
import org.atlasapi.media.product.ProductStore;
import org.atlasapi.media.segment.SegmentResolver;
import org.atlasapi.media.segment.SegmentWriter;
import org.atlasapi.persistence.content.ContentGroupResolver;
import org.atlasapi.persistence.content.ContentGroupWriter;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.KnownTypeContentResolver;
import org.atlasapi.persistence.content.ScheduleResolver;
import org.atlasapi.persistence.content.mongo.LastUpdatedContentFinder;
import org.atlasapi.persistence.content.people.ItemsPeopleWriter;
import org.atlasapi.persistence.content.schedule.mongo.ScheduleWriter;
import org.atlasapi.persistence.event.RecentChangeStore;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.atlasapi.persistence.shorturls.ShortUrlSaver;
import org.atlasapi.persistence.topic.TopicContentLister;
import org.atlasapi.persistence.topic.TopicQueryResolver;
import org.atlasapi.persistence.topic.TopicStore;

public interface ContentPersistenceModule {

    ContentGroupWriter contentGroupWriter();
    
    ContentGroupResolver contentGroupResolver();
    
	ContentWriter contentWriter();
	
	ItemsPeopleWriter itemsPeopleWriter();
	
	ContentResolver contentResolver();
	
	KnownTypeContentResolver knownTypeContentResolver();
	
	LookupEntryStore lookupStore();
	
	TopicStore topicStore();
	
	TopicQueryResolver topicQueryResolver();

	ShortUrlSaver shortUrlSaver();
	
	SegmentWriter segmentWriter();
	
	SegmentResolver segmentResolver();
	
	ProductStore productStore();
	
	ProductResolver productResolver();
	
	ChannelResolver channelResolver();
	
	ScheduleResolver scheduleResolver();
	
	ScheduleWriter scheduleWriter();
	
	LastUpdatedContentFinder lastUpdatedContentFinder();
	
	TopicContentLister topicContentLister();
	
    RecentChangeStore recentChangesStore();
}
