package org.atlasapi.persistence.content.organisation;

import static com.google.common.base.Preconditions.checkNotNull;

import java.math.BigInteger;
import java.util.UUID;

import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.Organisation;
import org.atlasapi.messaging.v3.EntityUpdatedMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.queue.MessageSender;
import com.metabroadcast.common.time.SystemClock;
import com.metabroadcast.common.time.Timestamper;

public class QueueingOrganisationStore implements OrganisationStore {
    private final static Logger log = LoggerFactory.getLogger(QueueingOrganisationStore.class);
    private final OrganisationStore delegate;
    private final MessageSender<EntityUpdatedMessage> sender;
    private final SubstitutionTableNumberCodec entityIdCodec;
    private final Timestamper clock;

    public QueueingOrganisationStore(MessageSender<EntityUpdatedMessage> sender, OrganisationStore delegate) {
        this(checkNotNull(sender), checkNotNull(delegate), new SystemClock(), SubstitutionTableNumberCodec.lowerCaseOnly());
    }

    public QueueingOrganisationStore(MessageSender<EntityUpdatedMessage> sender, OrganisationStore delegate,
            Timestamper timestamper, SubstitutionTableNumberCodec entityIdCodec) {
        this.delegate = checkNotNull(delegate);
        this.sender = checkNotNull(sender);
        this.clock = checkNotNull(timestamper);
        this.entityIdCodec = checkNotNull(entityIdCodec);
    }

    @Override
    public void updateOrganisationItems(Organisation organisation) {
        delegate.updateOrganisationItems(organisation);
        enqueueMessageUpdatedMessage(organisation);
    }

    @Override
    public void createOrUpdateOrganisation(Organisation organisation) {
        delegate.createOrUpdateOrganisation(organisation);
        enqueueMessageUpdatedMessage(organisation);
    }

    @Override
    public Optional<Organisation> organisation(String uri) {
        return delegate.organisation(uri);
    }

    @Override
    public Optional<Organisation> organisation(Long id) {
        return delegate.organisation(id);
    }

    @Override
    public Iterable<Organisation> organisations(Iterable<LookupRef> lookupRefs) {
        return delegate.organisations(lookupRefs);
    }

    private void enqueueMessageUpdatedMessage(final Organisation organisation) {
        try {
            sender.sendMessage(createEntityUpdatedMessage(organisation));
        } catch (Exception e) {
            log.error("update message failed: " + organisation, e);
        }
    }

    private EntityUpdatedMessage createEntityUpdatedMessage(Organisation organisation) {
        return new EntityUpdatedMessage(
                UUID.randomUUID().toString(),
                clock.timestamp(),
                entityIdCodec.encode(BigInteger.valueOf(organisation.getId())),
                organisation.getClass().getSimpleName().toLowerCase(),
                organisation.getPublisher().key());
    }
}
