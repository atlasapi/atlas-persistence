package org.atlasapi.persistence.content;

import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.lookup.LookupResolver;

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

public class LookupResolvingContentResolver implements ContentResolver {

    private final KnownTypeContentResolver knownTypeResolver;
    private final LookupResolver lookupResolver;

    public LookupResolvingContentResolver(KnownTypeContentResolver knownTypeResolver, LookupResolver lookupResolver) {
        this.knownTypeResolver = knownTypeResolver;
        this.lookupResolver = lookupResolver;
    }
    
    @Override
    public ResolvedContent findByCanonicalUris(Iterable<String> canonicalUris) {
    	ImmutableSet<LookupRef> resolvedLookups = ImmutableSet.copyOf(Iterables.filter(Iterables.transform(canonicalUris, lookup), Predicates.notNull()));
        ResolvedContent resolvedContent = knownTypeResolver.findByLookupRefs(resolvedLookups);
        return resolvedContent.copyWithAllRequestedUris(canonicalUris);
    }

    private Function<String, LookupRef> lookup = new Function<String, LookupRef>() {
        @Override
        public LookupRef apply(String input) {
            return lookupResolver.lookup(input);
        }
    };
}
