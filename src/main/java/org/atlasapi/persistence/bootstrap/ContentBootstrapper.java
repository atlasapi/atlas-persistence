/* Copyright 2009 Meta Broadcast Ltd

 Licensed under the Apache License, Version 2.0 (the "License"); you
 may not use this file except in compliance with the License. You may
 obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 implied. See the License for the specific language governing
 permissions and limitations under the License. */
package org.atlasapi.persistence.bootstrap;

import static com.google.common.base.Predicates.notNull;
import static org.atlasapi.persistence.content.listing.ContentListingCriteria.defaultCriteria;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Person;
import org.atlasapi.persistence.content.ContentCategory;
import org.atlasapi.persistence.content.people.PeopleLister;
import org.atlasapi.persistence.content.PeopleListerListener;
import org.atlasapi.persistence.content.listing.ContentLister;
import org.atlasapi.persistence.content.listing.ContentListingProgress;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import java.util.concurrent.locks.ReentrantLock;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelGroup;
import org.atlasapi.media.entity.ContentGroup;
import org.atlasapi.media.entity.Topic;
import org.atlasapi.media.product.Product;
import org.atlasapi.media.segment.Segment;
import org.atlasapi.persistence.content.ContentGroupLister;
import org.atlasapi.persistence.media.channel.ChannelGroupLister;
import org.atlasapi.persistence.media.channel.ChannelLister;
import org.atlasapi.persistence.media.product.ProductLister;
import org.atlasapi.persistence.media.segment.SegmentLister;
import org.atlasapi.persistence.topic.TopicLister;

public class ContentBootstrapper {

    private static final Log log = LogFactory.getLog(ContentBootstrapper.class);
    //
    private final ReentrantLock boostrapLock = new ReentrantLock();
    private volatile boolean bootstrapping;
    private volatile String destination;
    //
    private ContentLister[] contentListers = new ContentLister[0];
    private PeopleLister[] peopleListers = new PeopleLister[0];
    private ChannelLister[] channelListers = new ChannelLister[0];
    private ProductLister[] productListers = new ProductLister[0];
    private SegmentLister[] segmentListers = new SegmentLister[0];
    private TopicLister[] topicListers = new TopicLister[0];
    private ContentGroupLister[] contentGroupListers = new ContentGroupLister[0];
    private ChannelGroupLister[] channelGroupListers = new ChannelGroupLister[0];

    public ContentBootstrapper withContentListers(ContentLister... contentListers) {
        this.contentListers = contentListers;
        return this;
    }

    public ContentBootstrapper withPeopleListers(PeopleLister... peopleListers) {
        this.peopleListers = peopleListers;
        return this;
    }

    public ContentBootstrapper withChannelListers(ChannelLister... channelListers) {
        this.channelListers = channelListers;
        return this;
    }

    public ContentBootstrapper withProductListers(ProductLister... productListers) {
        this.productListers = productListers;
        return this;
    }

    public ContentBootstrapper withSegmentListers(SegmentLister... segmentListers) {
        this.segmentListers = segmentListers;
        return this;
    }

    public ContentBootstrapper withTopicListers(TopicLister... topicListers) {
        this.topicListers = topicListers;
        return this;
    }

    public ContentBootstrapper withContentGroupListers(ContentGroupLister... contentGroupListers) {
        this.contentGroupListers = contentGroupListers;
        return this;
    }

    public ContentBootstrapper withChannelGroupListers(ChannelGroupLister... channelGroupListers) {
        this.channelGroupListers = channelGroupListers;
        return this;
    }

    public boolean loadAllIntoListener(final ChangeListener listener) {
        if (boostrapLock.tryLock()) {
            try {
                bootstrapping = true;
                destination = listener.getClass().toString();
                listener.beforeChange();
                try {
                    log.info("Bootstrapping contents...");
                    int processedContents = bootstrapContent(listener);
                    log.info(String.format("Finished bootstrapping %s contents.", processedContents));

                    log.info("Bootstrapping content groups...");
                    int processedContentGroups = bootstrapContentGroups(listener);
                    log.info(String.format("Finished bootstrapping %s content groups!", processedContentGroups));

                    log.info("Bootstrapping people...");
                    int processedPeople = bootstrapPeople(listener);
                    log.info(String.format("Finished bootstrapping %s people!", processedPeople));

                    log.info("Bootstrapping products...");
                    int processedProducts = bootstrapProducts(listener);
                    log.info(String.format("Finished bootstrapping %s products!", processedProducts));

                    log.info("Bootstrapping segments...");
                    int processedSegments = bootstrapSegments(listener);
                    log.info(String.format("Finished bootstrapping %s segments!", processedSegments));

                    log.info("Bootstrapping channels...");
                    int processedChannels = bootstrapChannels(listener);
                    log.info(String.format("Finished bootstrapping %s channels!", processedChannels));

                    log.info("Bootstrapping channel groups...");
                    int processedChannelGroups = bootstrapChannelGroups(listener);
                    log.info(String.format("Finished bootstrapping %s channel groups!", processedChannelGroups));

                    log.info("Bootstrapping topics...");
                    int processedTopics = bootstrapTopics(listener);
                    log.info(String.format("Finished bootstrapping %s topics!", processedTopics));
                } catch (Exception ex) {
                    throw new RuntimeException(ex.getMessage(), ex);
                } finally {
                    listener.afterChange();
                }
            } finally {
                bootstrapping = false;
                boostrapLock.unlock();
            }
            return true;
        } else {
            return false;
        }
    }

    private int bootstrapContent(final ChangeListener listener) throws RuntimeException {
        int processed = 0;
        for (ContentLister lister : contentListers) {
            List<ContentCategory> contentCategories = Lists.newArrayList(ContentCategory.TOP_LEVEL_CONTENT);
            contentCategories.remove(ContentCategory.CONTENT_GROUP);
            //
            Iterator<Content> content = lister.listContent(defaultCriteria().forContent(contentCategories).build());
            Iterator<List<Content>> partitionedContent = Iterators.paddedPartition(content, 100);
            while (partitionedContent.hasNext()) {
                List<Content> partition = ImmutableList.copyOf(Iterables.filter(partitionedContent.next(), notNull()));
                try {
                    listener.onChange(partition);
                    processed += partition.size();
                } catch (RuntimeException ex) {
                    log.warn(ex.getMessage(), ex);
                    throw ex;
                }
                if (log.isInfoEnabled()) {
                    log.info(String.format("%s content processed: %s", processed, ContentListingProgress.progressFrom(Iterables.getLast(partition))));
                }
            }
        }
        return processed;
    }

    private int bootstrapPeople(final ChangeListener listener) {
        final AtomicInteger processed = new AtomicInteger(0);
        for (PeopleLister lister : peopleListers) {
            lister.list(new PeopleListerListener() {

                @Override
                public void personListed(Person person) {
                    try {
                        listener.onChange(ImmutableList.of(person));
                        processed.incrementAndGet();
                    } catch (RuntimeException ex) {
                        log.warn(ex.getMessage(), ex);
                        throw ex;
                    }
                }
            });
        }
        return processed.get();
    }

    private int bootstrapChannels(final ChangeListener listener) throws RuntimeException {
        int processed = 0;
        for (ChannelLister lister : channelListers) {
            for (Iterable<Channel> channels : Iterables.partition(lister.all(), 100)) {
                listener.onChange(channels);
                processed += Iterables.size(channels);
            }
        }
        return processed;
    }

    private int bootstrapProducts(final ChangeListener listener) throws RuntimeException {
        int processed = 0;
        for (ProductLister lister : productListers) {
            for (Iterable<Product> products : Iterables.partition(lister.products(), 100)) {
                listener.onChange(products);
                processed += Iterables.size(products);
            }
        }
        return processed;
    }

    private int bootstrapSegments(final ChangeListener listener) throws RuntimeException {
        int processed = 0;
        for (SegmentLister lister : segmentListers) {
            for (Iterable<Segment> segments : Iterables.partition(lister.segments(), 100)) {
                listener.onChange(segments);
                processed += Iterables.size(segments);
            }
        }
        return processed;
    }

    private int bootstrapTopics(final ChangeListener listener) throws RuntimeException {
        int processed = 0;
        for (TopicLister lister : topicListers) {
            for (Iterable<Topic> topics : Iterables.partition(lister.topics(), 100)) {
                listener.onChange(topics);
                processed += Iterables.size(topics);
            }
        }
        return processed;
    }

    private int bootstrapContentGroups(final ChangeListener listener) throws RuntimeException {
        int processed = 0;
        for (ContentGroupLister lister : contentGroupListers) {
            for (Iterable<ContentGroup> contentGroups : Iterables.partition(lister.findAll(), 100)) {
                listener.onChange(contentGroups);
                processed += Iterables.size(contentGroups);
            }
        }
        return processed;
    }

    private int bootstrapChannelGroups(final ChangeListener listener) throws RuntimeException {
        int processed = 0;
        for (ChannelGroupLister lister : channelGroupListers) {
            for (Iterable<ChannelGroup> channelGroups : Iterables.partition(lister.channelGroups(), 100)) {
                listener.onChange(channelGroups);
                processed += Iterables.size(channelGroups);
            }
        }
        return processed;
    }

    public boolean isBootstrapping() {
        return bootstrapping;
    }

    public String getDestination() {
        return destination;
    }
}
