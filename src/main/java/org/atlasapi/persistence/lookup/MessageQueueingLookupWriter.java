package org.atlasapi.persistence.lookup;

import java.util.Set;
import java.util.UUID;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;

import org.atlasapi.equiv.ContentRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.messaging.v3.EntityUpdatedMessage;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.serialization.json.JsonFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.MessageCreator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.metabroadcast.common.time.SystemClock;

public class MessageQueueingLookupWriter implements LookupWriter {

    private static final Logger log = LoggerFactory.getLogger(MessageQueueingLookupWriter.class);
    private final ObjectMapper mapper = JsonFactory.makeJsonMapper();
    private final JmsTemplate template;
    private final LookupWriter delegate;
    private final SystemClock clock;

    public MessageQueueingLookupWriter(JmsTemplate template, LookupWriter delegate) {
        this.template = template;
        this.delegate = delegate;
        this.clock = new SystemClock();
    }
    
    @Override
    public Optional<Set<LookupEntry>> writeLookup(ContentRef subjectUri, Iterable<ContentRef> equivalentUris,
            Set<Publisher> publishers) {
        Optional<Set<LookupEntry>> updated = delegate.writeLookup(subjectUri, equivalentUris, publishers);
        if (updated.isPresent()) {
            enqueueUpdates(updated.get());
        }
        return updated;
    }

    private void enqueueUpdates(Set<LookupEntry> updates) {
        for (LookupEntry lookupEntry : updates) {
            enqueueUpdate(lookupEntry);
        }
    }

    private void enqueueUpdate(final LookupEntry lookupEntry) {
        template.send(new MessageCreator() {
            @Override
            public Message createMessage(Session session) throws JMSException {
                return session.createTextMessage(serialize(createEntityUpdatedMessage(lookupEntry)));
            }
        });
    }

    private EntityUpdatedMessage createEntityUpdatedMessage(LookupEntry lookupEntry) {
        return new EntityUpdatedMessage(
                UUID.randomUUID().toString(),
                clock.now().getMillis(),
                lookupEntry.id().toString(),
                lookupEntry.getClass().getSimpleName().toLowerCase(),
                lookupEntry.lookupRef().publisher().key());
    }

    private String serialize(final EntityUpdatedMessage message) {
        String result = null;
        try {
            result = mapper.writeValueAsString(message);
        } catch (Exception e) {
            log.error(message.getEntityId(), e);
        }
        return result;
    }
    
}
