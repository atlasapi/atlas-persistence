package org.atlasapi.persistence.content.people;

import static com.google.common.base.Preconditions.checkNotNull;

import org.atlasapi.media.entity.Person;
import org.atlasapi.persistence.content.PeopleListerListener;

import com.metabroadcast.common.ids.IdGenerator;


public class IdSettingPersonStore implements PersonStore {

    private final PersonStore delegate;
    private final IdGenerator idGenerator;

    public IdSettingPersonStore(PersonStore delegate,
            IdGenerator idGenerator) {
        this.delegate = checkNotNull(delegate);
        this.idGenerator = checkNotNull(idGenerator);
    }

    @Override
    public void updatePersonItems(Person person) {
        delegate.updatePersonItems(generateOrRestoreId(person));
    }

    @Override
    public void createOrUpdatePerson(Person person) {
        delegate.createOrUpdatePerson(generateOrRestoreId(person));
    }

    private Person generateOrRestoreId(Person person) {
        Person existing = person(person.getCanonicalUri());
        if (existing != null && existing.getId() != null) {
            person.setId(existing.getId());
        } else {
            person.setId(idGenerator.generateRaw());
        }
        return person;
    }

    @Override
    public Person person(String uri) {
        return delegate.person(uri);
    }

    @Override
    public Person person(Long id) {
        return delegate.person(id);
    }

    @Override
    public void list(PeopleListerListener handler) {
        delegate.list(handler);
    }

}
