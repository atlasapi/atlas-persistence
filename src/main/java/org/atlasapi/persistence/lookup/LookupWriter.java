package org.atlasapi.persistence.lookup;

import java.util.Set;

import org.atlasapi.equiv.ContentRef;
import org.atlasapi.media.entity.Publisher;

public interface LookupWriter {

    void writeLookup(ContentRef subjectUri, Iterable<ContentRef> equivalentUris, Set<Publisher> publishers);

}
