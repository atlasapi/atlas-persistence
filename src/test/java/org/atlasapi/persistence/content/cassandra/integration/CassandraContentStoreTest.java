package org.atlasapi.persistence.content.cassandra.integration;

import com.google.common.collect.Lists;
import org.atlasapi.media.entity.Clip;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.KeyPhrase;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.RelatedLink;
import org.atlasapi.media.entity.TopicRef;
import org.atlasapi.persistence.content.cassandra.CassandraContentStore;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 */
public class CassandraContentStoreTest {

    private static final String HOST = "localhost";
    private static final int PORT = 9160;

    @Test
    public void testWriteItem() {
        CassandraContentStore writer = new CassandraContentStore(PORT, Lists.newArrayList(HOST));

        Item item = new Item();
        // Main properties:
        item.setCanonicalUri("www.acme.org/items/1");
        item.setId("1");
        // Detached properties:
        item.setClips(Lists.newArrayList(new Clip("www.acme.org/clips/1", "", Publisher.BBC)));
        item.setKeyPhrases(Lists.newArrayList(new KeyPhrase("?", Publisher.BBC)));
        item.setRelatedLinks(Lists.newArrayList(RelatedLink.unknownTypeLink("www.acme.org/links/1").build()));
        item.setTopicRefs(Lists.newArrayList(new TopicRef(1l, 1f, Boolean.TRUE)));

        writer.createOrUpdate(item);

        assertEquals(item, writer.findByCanonicalUris(Lists.newArrayList("www.acme.org/items/1")).getFirstValue().requireValue());
    }
}
