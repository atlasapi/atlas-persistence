package org.atlasapi.persistence.content;

import java.util.Map;
import java.util.Set;

import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.Annotation;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;

import com.google.common.base.Functions;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.metabroadcast.common.base.MorePredicates;

public class DefaultEquivalentContentResolver implements EquivalentContentResolver {

    private ContentResolver contentResolver;
    private LookupEntryStore entryStore;

    public DefaultEquivalentContentResolver(ContentResolver contentResolver, LookupEntryStore entryStore) {
        this.contentResolver = contentResolver;
        this.entryStore = entryStore;
    }
    
    @Override
    public EquivalentContent resolve(Iterable<String> uris, Set<Publisher> selectedSources, Set<Annotation> activeAnnotations) {
        Map<String, Iterable<String>> mapToEquivs = mapToEquivs(filter(entryStore.entriesForUris(uris), selectedSources), selectedSources);
        
        ResolvedContent resolvedContent = contentResolver.findByCanonicalUris(Iterables.concat(mapToEquivs.values()));
        
        EquivalentContent.Builder equivalentContent = EquivalentContent.builder();
        for (String uri : uris) {
            equivalentContent.put(uri, ImmutableSet.copyOf(Iterables.filter(resolvedContent.getResolvedResults(mapToEquivs.get(uri)), Content.class)));
        }
        return equivalentContent.build();
    }

    private Map<String, Iterable<String>> mapToEquivs(Iterable<LookupEntry> resolved, Set<Publisher> selectedSources) {
        Builder<String, Iterable<String>> builder = ImmutableMap.builder();
        for (LookupEntry entry : resolved) {
            builder.put(entry.uri(), Iterables.transform(Sets.filter(entry.equivalents(), MorePredicates.transformingPredicate(LookupRef.TO_SOURCE, Predicates.in(selectedSources))),LookupRef.TO_ID));
        }
        return builder.build();
    }

    private Iterable<LookupEntry> filter(Iterable<LookupEntry> entriesForUris, Set<Publisher> selectedSources) {
        return Iterables.filter(entriesForUris, MorePredicates.transformingPredicate(Functions.compose(LookupRef.TO_SOURCE, LookupEntry.TO_SELF), Predicates.in(selectedSources)));
    }

}
