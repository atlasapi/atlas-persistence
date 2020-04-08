package org.atlasapi.persistence.content;

import com.google.common.primitives.Longs;
import com.metabroadcast.common.queue.MessageSender;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.messaging.v3.EntityUpdatedMessage;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.runners.MockitoJUnitRunner;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class MessageQueueingContentWriterTest {

    @SuppressWarnings("unchecked")
    private final MessageSender<EntityUpdatedMessage> sender = 
        (MessageSender<EntityUpdatedMessage>) mock(MessageSender.class);
    private final ContentWriter delegate = mock(ContentWriter.class);
    private final ContentResolver resolver = mock(ContentResolver.class);
    private final MessageQueueingContentWriter writer 
        = new MessageQueueingContentWriter(sender, delegate, resolver);
    
    @Test
    public void testEnqueuesMessageWhenContentChanges() throws Exception {
        
        Episode episode = new Episode("uri","curie",Publisher.METABROADCAST);
        episode.setId(1225L);
        episode.setReadHash(null);
        
        writer.createOrUpdate(episode);
        
        verify(delegate).createOrUpdate(episode);
        
        ArgumentCaptor<EntityUpdatedMessage> creatorCaptor = ArgumentCaptor.forClass(EntityUpdatedMessage.class);
        
        verify(sender).sendMessage(creatorCaptor.capture(), eq(Longs.toByteArray(1225L)));
        
        EntityUpdatedMessage message = creatorCaptor.getValue();
        
        assertThat(message.getEntityId(), is("cyp"));
        
    }

}
