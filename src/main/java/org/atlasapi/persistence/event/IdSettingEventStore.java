package org.atlasapi.persistence.event;

import org.atlasapi.media.entity.Event;
import org.atlasapi.media.entity.Topic;

import com.metabroadcast.common.ids.IdGenerator;

import com.google.common.base.Optional;
import org.joda.time.DateTime;

import static com.google.common.base.Preconditions.checkNotNull;


public class IdSettingEventStore implements EventStore {

    private final EventStore delegate;
    private final IdGenerator idGenerator;
    
    public IdSettingEventStore(EventStore delegate, IdGenerator idGenerator) {
        this.delegate = checkNotNull(delegate);
        this.idGenerator = checkNotNull(idGenerator);
    }
    
    @Override
    public Event createOrUpdate(Event event) {
        return delegate.createOrUpdate(generateOrRestoreId(event));
    }

    @Override
    public Optional<Event> fetch(Long id) {
        return delegate.fetch(id);
    }

    @Override
    public Optional<Event> fetch(String uri) {
        return delegate.fetch(uri);
    }
    
    @Override
    public Iterable<Event> fetch(Optional<Topic> eventGroup, Optional<DateTime> from) {
        return delegate.fetch(eventGroup, from);
    }

    private Event generateOrRestoreId(Event event) {
        Optional<Event> existing = fetch(event.getCanonicalUri());
        if (existing.isPresent() && existing.get().getId() != null) {
            event.setId(existing.get().getId());
        } else {
            event.setId(idGenerator.generateRaw());
        }
        return event;
    }
}
