package org.atlasapi.persistence.content.mongo;

import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.persistence.content.ResolvedContent;

/**
 * Retrieve content from a known internal content table
 * @author Fred van den Driessche (fred@metabroadcast.com)
 *
 */
public interface KnownTypeContentResolver {

    ResolvedContent findByLookupRefs(Iterable<LookupRef> lookupRefs);
    
}
