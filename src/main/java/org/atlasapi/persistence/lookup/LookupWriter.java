package org.atlasapi.persistence.lookup;

import java.util.Set;

import org.atlasapi.equiv.ContentRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.lookup.entry.LookupEntry;

import com.google.common.base.Optional;

public interface LookupWriter {

    Optional<Set<LookupEntry>> writeLookup(ContentRef subjectUri, Iterable<ContentRef> equivalentUris, Set<Publisher> publishers);

}
