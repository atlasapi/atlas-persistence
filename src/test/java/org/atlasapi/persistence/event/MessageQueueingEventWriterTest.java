package org.atlasapi.persistence.event;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigInteger;

import org.atlasapi.media.entity.Event;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.messaging.v3.EntityUpdatedMessage;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.queue.MessageSender;
import com.metabroadcast.common.time.Timestamp;
import com.metabroadcast.common.time.Timestamper;

@RunWith(MockitoJUnitRunner.class)
public class MessageQueueingEventWriterTest {

    @Captor
    public ArgumentCaptor<EntityUpdatedMessage> messageCaptor;

    private MessageQueueingEventWriter messageQueueingEventWriter;

    private @Mock EventStore delegate;
    private @Mock MessageSender<EntityUpdatedMessage> sender;
    private @Mock Timestamper timestamper;
    private @Mock SubstitutionTableNumberCodec eventIdCodec;

    private Timestamp timestamp;
    private long id;
    private String entityId;

    @Before
    public void setUp() throws Exception {
        messageQueueingEventWriter = new MessageQueueingEventWriter(
                delegate, sender, timestamper, eventIdCodec
        );

        timestamp = Timestamp.of(0L);
        id = 0L;
        entityId = "entityId";
        when(timestamper.timestamp()).thenReturn(timestamp);
        when(eventIdCodec.encode(BigInteger.valueOf(id))).thenReturn(entityId);
    }

    @Test
    public void testEnqueueMessageWhenEventChanges() throws Exception {
        Event event = Event.builder().withTitle("title").withPublisher(Publisher.BBC).build();
        event.setId(id);

        messageQueueingEventWriter.createOrUpdate(event);

        verify(sender).sendMessage(messageCaptor.capture());

        EntityUpdatedMessage message = messageCaptor.getValue();
        assertThat(message.getEntityId(), is(entityId));
        assertThat(message.getEntitySource(), is(Publisher.BBC.key()));
        assertThat(message.getEntityType(), is(event.getClass().getSimpleName().toLowerCase()));
        assertThat(message.getTimestamp(), is(timestamp));
        assertThat(message.getMessageId(), not(nullValue()));
    }
}