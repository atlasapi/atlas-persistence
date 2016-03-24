package org.atlasapi.persistence.content;

import java.util.Set;

import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.output.Annotation;

public class FilterScheduleOnlyKnownTypeContentResolver implements KnownTypeContentResolver {
    
    private final KnownTypeContentResolver resolver;

    public FilterScheduleOnlyKnownTypeContentResolver(KnownTypeContentResolver resolver) {
        this.resolver = resolver;
    }

    @Override
    public ResolvedContent findByLookupRefs(Iterable<LookupRef> lookupRefs) {
        ResolvedContent resolvedContent = resolver.findByLookupRefs(lookupRefs);
        return resolvedContent.filterContent(FilterScheduleOnlyContentResolver.NOT_SCHEDULE_ONLY);
    }

    @Override
    public ResolvedContent findByLookupRefs(Iterable<LookupRef> lookupRefs,
            Set<Annotation> activeAnnotations) {
        ResolvedContent resolvedContent = resolver.findByLookupRefs(lookupRefs, activeAnnotations);
        return resolvedContent.filterContent(FilterScheduleOnlyContentResolver.NOT_SCHEDULE_ONLY);
    }
}
