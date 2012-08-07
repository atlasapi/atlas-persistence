package org.atlasapi.persistence;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.atlasapi.persistence.content.ContentGroupResolver;
import org.atlasapi.persistence.content.ContentGroupWriter;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.KnownTypeContentResolver;
import org.atlasapi.persistence.content.ScheduleResolver;
import org.atlasapi.persistence.content.cassandra.CassandraContentStore;
import org.atlasapi.persistence.content.listing.ContentLister;
import org.atlasapi.persistence.content.mongo.LastUpdatedContentFinder;
import org.atlasapi.persistence.content.people.ItemsPeopleWriter;
import org.atlasapi.persistence.content.schedule.mongo.ScheduleWriter;
import org.atlasapi.persistence.event.RecentChangeStore;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.atlasapi.persistence.media.channel.ChannelResolver;
import org.atlasapi.persistence.media.product.ProductResolver;
import org.atlasapi.persistence.media.product.ProductStore;
import org.atlasapi.persistence.media.segment.SegmentResolver;
import org.atlasapi.persistence.media.segment.SegmentWriter;
import org.atlasapi.persistence.shorturls.ShortUrlSaver;
import org.atlasapi.persistence.topic.TopicContentLister;
import org.atlasapi.persistence.topic.TopicQueryResolver;
import org.atlasapi.persistence.topic.TopicStore;

/**
 */
public class CassandraContentPersistenceModule implements ContentPersistenceModule {

    private final CassandraContentStore cassandraContentStore;

    public CassandraContentPersistenceModule(String seeds, int port, int connectionTimeout, int requestTimeout) {
        this.cassandraContentStore = new CassandraContentStore(Lists.newArrayList(Splitter.on(',').split(seeds)), port, Runtime.getRuntime().availableProcessors() * 10, connectionTimeout, requestTimeout);
    }

    @Override
    public ContentResolver contentResolver() {
        return cassandraContentStore;
    }

    @Override
    public ContentWriter contentWriter() {
        return cassandraContentStore;
    }

    @Override
    public ContentLister contentLister() {
        return cassandraContentStore;
    }

    @Override
    public ChannelResolver channelResolver() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public ContentGroupResolver contentGroupResolver() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public ContentGroupWriter contentGroupWriter() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public ItemsPeopleWriter itemsPeopleWriter() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public KnownTypeContentResolver knownTypeContentResolver() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public LastUpdatedContentFinder lastUpdatedContentFinder() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public LookupEntryStore lookupStore() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public ProductResolver productResolver() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public ProductStore productStore() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public RecentChangeStore recentChangesStore() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public ScheduleResolver scheduleResolver() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public ScheduleWriter scheduleWriter() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public SegmentResolver segmentResolver() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public SegmentWriter segmentWriter() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public ShortUrlSaver shortUrlSaver() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public TopicContentLister topicContentLister() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public TopicQueryResolver topicQueryResolver() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public TopicStore topicStore() {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
}
