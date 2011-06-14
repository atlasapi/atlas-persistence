package org.atlasapi.persistence.content;

import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.Identified;

import com.google.common.base.Predicate;
import com.metabroadcast.common.base.Maybe;

public class FilterScheduleOnlyContentResolver implements ContentResolver {

    private final ContentResolver contentResovler;

    public FilterScheduleOnlyContentResolver(ContentResolver contentResovler) {
        this.contentResovler = contentResovler;
    }

    @Override
    public ResolvedContent findByCanonicalUris(Iterable<String> lookups) {
        ResolvedContent resolvedContent = contentResovler.findByCanonicalUris(lookups);
        return resolvedContent.filterContent(REMOVE_SCHEDULE_ONLY);
    }
    
    private final static Predicate<Maybe<Identified>> REMOVE_SCHEDULE_ONLY = new Predicate<Maybe<Identified>>() {
        @Override
        public boolean apply(Maybe<Identified> input) {
            if (input.hasValue() && input.requireValue() instanceof Described && ((Described) input.requireValue()).isScheduleOnly()) {
                return false;
            }
            
            return true;
        }
    };
}
