package org.atlasapi.persistence.lookup;

import java.util.List;

import org.atlasapi.application.ApplicationConfiguration;

public class MongoLookupResolver implements LookupResolver {

    @Override
    public List<Equivalent> lookup(String id, ApplicationConfiguration config) {
        return null;
    }

    @Override
    public List<Equivalent> lookupGroup(String id, ApplicationConfiguration config) {
        return null;
    }

}
