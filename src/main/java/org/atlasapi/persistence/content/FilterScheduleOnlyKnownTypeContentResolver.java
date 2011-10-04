package org.atlasapi.persistence.content;

import org.atlasapi.persistence.lookup.entry.LookupRef;

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
}
