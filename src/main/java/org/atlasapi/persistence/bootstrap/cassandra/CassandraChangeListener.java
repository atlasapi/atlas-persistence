package org.atlasapi.persistence.bootstrap.cassandra;

import java.util.concurrent.ThreadPoolExecutor;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelGroup;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.ContentGroup;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Person;
import org.atlasapi.media.entity.Topic;
import org.atlasapi.media.product.Product;
import org.atlasapi.media.segment.Segment;
import org.atlasapi.persistence.bootstrap.AbstractMultiThreadedChangeListener;
import org.atlasapi.persistence.content.cassandra.CassandraContentGroupStore;
import org.atlasapi.persistence.content.cassandra.CassandraContentStore;
import org.atlasapi.persistence.content.cassandra.CassandraProductStore;
import org.atlasapi.persistence.content.people.cassandra.CassandraPersonStore;
import org.atlasapi.persistence.media.channel.cassandra.CassandraChannelGroupStore;
import org.atlasapi.persistence.media.channel.cassandra.CassandraChannelStore;
import org.atlasapi.persistence.media.segment.cassandra.CassandraSegmentStore;
import org.atlasapi.persistence.topic.cassandra.CassandraTopicStore;

/**
 */
public class CassandraChangeListener extends AbstractMultiThreadedChangeListener {

    private CassandraContentStore cassandraContentStore;
    private CassandraChannelGroupStore cassandraChannelGroupStore;
    private CassandraChannelStore cassandraChannelStore;
    private CassandraContentGroupStore cassandraContentGroupStore;
    private CassandraPersonStore cassandraPersonStore;
    private CassandraProductStore cassandraProductStore;
    private CassandraSegmentStore cassandraSegmentStore;
    private CassandraTopicStore cassandraTopicStore;

    public CassandraChangeListener(int concurrencyLevel) {
        super(concurrencyLevel);
    }

    public CassandraChangeListener(ThreadPoolExecutor executor) {
        super(executor);
    }

    public void setCassandraChannelGroupStore(CassandraChannelGroupStore cassandraChannelGroupStore) {
        this.cassandraChannelGroupStore = cassandraChannelGroupStore;
    }

    public void setCassandraChannelStore(CassandraChannelStore cassandraChannelStore) {
        this.cassandraChannelStore = cassandraChannelStore;
    }

    public void setCassandraContentGroupStore(CassandraContentGroupStore cassandraContentGroupStore) {
        this.cassandraContentGroupStore = cassandraContentGroupStore;
    }

    public void setCassandraContentStore(CassandraContentStore cassandraContentStore) {
        this.cassandraContentStore = cassandraContentStore;
    }

    public void setCassandraPersonStore(CassandraPersonStore cassandraPersonStore) {
        this.cassandraPersonStore = cassandraPersonStore;
    }

    public void setCassandraProductStore(CassandraProductStore cassandraProductStore) {
        this.cassandraProductStore = cassandraProductStore;
    }

    public void setCassandraSegmentStore(CassandraSegmentStore cassandraSegmentStore) {
        this.cassandraSegmentStore = cassandraSegmentStore;
    }

    public void setCassandraTopicStore(CassandraTopicStore cassandraTopicStore) {
        this.cassandraTopicStore = cassandraTopicStore;
    }

    @Override
    protected void onChange(Identified change) {
        if (change instanceof Item) {
            cassandraContentStore.createOrUpdate((Item) change);
        } else if (change instanceof Container) {
            cassandraContentStore.createOrUpdate((Container) change);
        } else if (change instanceof Person) {
            cassandraPersonStore.createOrUpdatePerson((Person) change);
        } else if (change instanceof Channel) {
            cassandraChannelStore.write((Channel) change);
        } else if (change instanceof Product) {
            cassandraProductStore.store((Product) change);
        } else if (change instanceof Segment) {
            cassandraSegmentStore.write((Segment) change);
        } else if (change instanceof Topic) {
            cassandraTopicStore.write((Topic) change);
        } else if (change instanceof ContentGroup) {
            cassandraContentGroupStore.createOrUpdate((ContentGroup) change);
        } else if (change instanceof ChannelGroup) {
            cassandraChannelGroupStore.store((ChannelGroup) change);
        }
    }
}
