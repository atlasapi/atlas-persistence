package org.atlasapi.persistence.content;

import static org.mockito.Matchers.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import javax.jms.Session;

import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Publisher;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.MessageCreator;

@RunWith(MockitoJUnitRunner.class)
public class MessageQueueingContentWriterTest {

    private final JmsTemplate template = mock(JmsTemplate.class);
    private final ContentWriter delegate = mock(ContentWriter.class);    
    private final MessageQueueingContentWriter writer 
        = new MessageQueueingContentWriter(template, delegate);
    
    @Test
    public void testEnqueuesMessageWhenContentChanges() throws Exception {
        
        Episode episode = new Episode("uri","curie",Publisher.METABROADCAST);
        episode.setId(54321L);
        episode.setReadHash(null);
        
        writer.createOrUpdate(episode);
        
        verify(delegate).createOrUpdate(episode);
        
        ArgumentCaptor<MessageCreator> creatorCaptor = ArgumentCaptor.forClass(MessageCreator.class);
        
        verify(template).send(creatorCaptor.capture());
        
        MessageCreator creator = creatorCaptor.getValue();
        
        Session session = mock(Session.class);
        creator.createMessage(session);
        
        verify(session).createTextMessage(contains("54321"));
        
    }

}
