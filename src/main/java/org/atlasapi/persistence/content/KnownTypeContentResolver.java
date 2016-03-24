package org.atlasapi.persistence.content;

import java.util.Set;

import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.output.Annotation;

/**
 * Retrieve content from a known internal content table
 * @author Fred van den Driessche (fred@metabroadcast.com)
 *
 */
public interface KnownTypeContentResolver {

    ResolvedContent findByLookupRefs(Iterable<LookupRef> lookupRefs);
    ResolvedContent findByLookupRefs(Iterable<LookupRef> lookupRefs, Set<Annotation> activeAnnotations);
    
}
