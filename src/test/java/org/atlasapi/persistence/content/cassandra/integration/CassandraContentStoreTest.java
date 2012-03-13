package org.atlasapi.persistence.content.cassandra.integration;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.metabroadcast.common.intl.Countries;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import org.atlasapi.media.entity.Actor;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Clip;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.KeyPhrase;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.RelatedLink;
import org.atlasapi.media.entity.Series;
import org.atlasapi.media.entity.TopicRef;
import org.atlasapi.persistence.content.cassandra.CassandraContentStore;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Before;

/**
 */
public class CassandraContentStoreTest {

    private static final String HOST = "localhost";
    private static final int PORT = 9160;

    @Before
    public void onSetUp() throws Exception {
        AstyanaxContext<Keyspace> context = new AstyanaxContext.Builder().forCluster(CassandraContentStore.CLUSTER).forKeyspace(CassandraContentStore.KEYSPACE).
                withAstyanaxConfiguration(new AstyanaxConfigurationImpl().setDiscoveryType(NodeDiscoveryType.NONE)).
                withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl(CassandraContentStore.CLUSTER).setPort(PORT).setMaxConnsPerHost(1).
                setSeeds(HOST)).
                withConnectionPoolMonitor(new CountingConnectionPoolMonitor()).
                buildKeyspace(ThriftFamilyFactory.getInstance());

        context.start();

        context.getEntity().truncateColumnFamily(CassandraContentStore.CONTENT_CF);
    }

    @Test
    public void testWriteAndReadContent() {
        CassandraContentStore writer = new CassandraContentStore(PORT, Lists.newArrayList(HOST));

        Item item = new Item();
        item.setCanonicalUri("www.acme.org/content/1");
        item.setId("1");
        item.setCountriesOfOrigin(ImmutableSet.of(Countries.GB));
        item.addPerson(new Actor("www.acme.org/actors/1", "", Publisher.BBC));
        
        item.setPublisher(Publisher.BBC);
        item.setClips(Lists.newArrayList(new Clip("www.acme.org/clips/1", "", Publisher.BBC)));
        item.setKeyPhrases(Lists.newArrayList(new KeyPhrase("?", Publisher.BBC)));
        item.setRelatedLinks(Lists.newArrayList(RelatedLink.unknownTypeLink("www.acme.org/links/1").build()));
        item.setTopicRefs(Lists.newArrayList(new TopicRef(1l, 1f, Boolean.TRUE)));

        writer.createOrUpdate(item);

        assertEquals(item, writer.findByCanonicalUris(Lists.newArrayList("www.acme.org/content/1")).getFirstValue().requireValue());
    }

    @Test
    public void testWriteAndReadContainer() {
        CassandraContentStore writer = new CassandraContentStore(PORT, Lists.newArrayList(HOST));

        Brand brand = new Brand();
        brand.setCanonicalUri("www.acme.org/content/2");
        brand.setId("2");
        brand.setPublisher(Publisher.BBC);
        
        brand.setClips(Lists.newArrayList(new Clip("www.acme.org/clips/2", "", Publisher.BBC)));
        brand.setKeyPhrases(Lists.newArrayList(new KeyPhrase("?", Publisher.BBC)));
        brand.setRelatedLinks(Lists.newArrayList(RelatedLink.unknownTypeLink("www.acme.org/links/2").build()));
        brand.setTopicRefs(Lists.newArrayList(new TopicRef(1l, 1f, Boolean.TRUE)));

        writer.createOrUpdate(brand);

        assertEquals(brand, writer.findByCanonicalUris(Lists.newArrayList("www.acme.org/content/2")).getFirstValue().requireValue());
    }

    @Test
    public void testWriteAndReadContainerWithChildContent() {
        CassandraContentStore writer = new CassandraContentStore(PORT, Lists.newArrayList(HOST));

        Brand brand = new Brand();
        brand.setCanonicalUri("www.acme.org/content/3");
        brand.setId("3");
        brand.setPublisher(Publisher.BBC);

        Series series = new Series();
        series.setCanonicalUri("www.acme.org/content/4");
        series.setId("4");
        
        Episode episode = new Episode();
        episode.setCanonicalUri("www.acme.org/content/5");
        episode.setId("5");
        episode.setClips(Lists.newArrayList(new Clip("www.acme.org/clips/5", "", Publisher.BBC)));
        episode.setKeyPhrases(Lists.newArrayList(new KeyPhrase("?", Publisher.BBC)));
        episode.setRelatedLinks(Lists.newArrayList(RelatedLink.unknownTypeLink("www.acme.org/links/5").build()));
        episode.setTopicRefs(Lists.newArrayList(new TopicRef(1l, 1f, Boolean.TRUE)));

        brand.setChildRefs(ImmutableList.of(series.childRef()));
        series.setParentRef(ParentRef.parentRefFrom(brand));
        series.setChildRefs(ImmutableList.of(episode.childRef()));
        episode.setParentRef(ParentRef.parentRefFrom(series));

        writer.createOrUpdate(brand);
        writer.createOrUpdate(series);
        writer.createOrUpdate(episode);

        Brand readBrand = (Brand) writer.findByCanonicalUris(Lists.newArrayList("www.acme.org/content/3")).getFirstValue().requireValue();
        assertEquals(brand, readBrand);
        assertEquals(ImmutableList.of(series.childRef()), readBrand.getChildRefs());
        
        Series readSeries = (Series) writer.findByCanonicalUris(Lists.newArrayList("www.acme.org/content/4")).getFirstValue().requireValue();
        assertEquals(series, readSeries);
        assertEquals(ImmutableList.of(episode.childRef()), readSeries.getChildRefs());
        
        Episode readEpisode = (Episode) writer.findByCanonicalUris(Lists.newArrayList("www.acme.org/content/5")).getFirstValue().requireValue();
        assertEquals(episode, readEpisode);
    }
}
