package org.atlasapi.persistence.content.organisation;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigInteger;

import org.atlasapi.media.entity.Organisation;
import org.atlasapi.media.entity.Person;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.messaging.v3.EntityUpdatedMessage;

import com.google.common.primitives.Longs;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.queue.MessageSender;
import com.metabroadcast.common.time.Timestamp;
import com.metabroadcast.common.time.Timestamper;

@RunWith(MockitoJUnitRunner.class)
public class QueueingOrganisationStoreTest {
    @Captor
    public ArgumentCaptor<EntityUpdatedMessage> messageCaptor;
    @Captor
    public ArgumentCaptor<byte[]> byteCaptor;

    private QueueingOrganisationStore store;

    private @Mock OrganisationStore delegate;
    private @Mock MessageSender<EntityUpdatedMessage> sender;
    private @Mock Timestamper timestamper;
    private @Mock SubstitutionTableNumberCodec eventIdCodec;
    private Timestamp timestamp;
    private long id;
    private String entityId;

    @Before
    public void setUp() throws Exception {
        store = new QueueingOrganisationStore(
                sender, delegate, timestamper, eventIdCodec
        );

        timestamp = Timestamp.of(0L);
        id = 1L;
        entityId = "entityId";
        when(timestamper.timestamp()).thenReturn(timestamp);
        when(eventIdCodec.encode(BigInteger.valueOf(id))).thenReturn(entityId);
    }

    @Test
    public void testEnqueueMessageWhenOrganisationChanges() throws Exception {
        Organisation organisation = new Organisation();
        organisation.setId(id);
        organisation.setPublisher(Publisher.BBC);
        organisation.setAlternativeTitles(ImmutableSet.of("title1"));
        organisation.setMembers(ImmutableList.of(new Person().withName("person1")));

        store.createOrUpdateOrganisation(organisation);

        verify(sender).sendMessage(messageCaptor.capture(), byteCaptor.capture());

        EntityUpdatedMessage message = messageCaptor.getValue();
        assertThat(message.getEntityId(), is(entityId));
        assertThat(message.getEntitySource(), is(Publisher.BBC.key()));
        assertThat(message.getEntityType(), is(organisation.getClass().getSimpleName().toLowerCase()));
        assertThat(message.getTimestamp(), is(timestamp));
        assertThat(message.getMessageId(), not(nullValue()));
        byte[] bytes = byteCaptor.getValue();
        assertThat(bytes, is(Longs.toByteArray(organisation.getId())));
    }
}