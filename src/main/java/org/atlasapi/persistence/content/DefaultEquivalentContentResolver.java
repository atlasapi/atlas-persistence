package org.atlasapi.persistence.content;

import static org.atlasapi.media.entity.LookupRef.TO_SOURCE;

import java.util.List;
import java.util.Set;

import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.Annotation;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.base.MoreOrderings;
import com.metabroadcast.common.base.MorePredicates;

public class DefaultEquivalentContentResolver implements EquivalentContentResolver {

    private ContentResolver contentResolver;
    private LookupEntryStore lookupResolver;

    public DefaultEquivalentContentResolver(ContentResolver contentResolver, LookupEntryStore lookupResolver) {
        this.contentResolver = contentResolver;
        this.lookupResolver = lookupResolver;
    }
    
    @Override
    public EquivalentContent resolveUris(Iterable<String> uris, List<Publisher> selectedSources, Set<Annotation> activeAnnotations, boolean withAliases) {
        Iterable<LookupEntry> entries = lookupResolver.entriesForIdentifiers(uris, withAliases);
        return filterAndResolveEntries(entries, uris, selectedSources);
    }
    
    @Override
    public EquivalentContent resolveIds(Iterable<Long> ids, List<Publisher> selectedSources, Set<Annotation> activeAnnotations) {
        Iterable<LookupEntry> entries = lookupResolver.entriesForIds(ids);
        Set<String> uris = Sets.newHashSet();
        for (LookupEntry entry : entries) {
            uris.add(entry.uri());
        }
        return filterAndResolveEntries(entries, uris, selectedSources);
    }

    protected EquivalentContent filterAndResolveEntries(Iterable<LookupEntry> entries, Iterable<String> uris, List<Publisher> selectedSources) {
        Iterable<LookupEntry> selectedEntries = filter(entries, selectedSources);
        
        if (Iterables.isEmpty(selectedEntries)) {
            return EquivalentContent.empty();
        }
        
        ImmutableSetMultimap<String, String> uriToEquivs = uriToEquivs(selectedEntries, selectedSources);
        
        ResolvedContent resolvedContent = contentResolver.findByCanonicalUris(uriToEquivs.values());
        
        EquivalentContent.Builder equivalentContent = EquivalentContent.builder();
        for (String uri : uris) {
            ImmutableSet<String> equivUris = uriToEquivs.get(uri);
            Iterable<Content> content = equivContent(equivUris, resolvedContent);
            equivalentContent.putEquivalents(uri, content);
        }
        return equivalentContent.build();
    }

    protected List<Content> equivContent(Set<String> equivUris, ResolvedContent resolved) {
        if (equivUris == null) {
            return ImmutableList.of();
        }
        ImmutableList.Builder<Content> builder = ImmutableList.builder();
        for (String uri : equivUris) {
            Maybe<Identified> resolvedContent = resolved.get(uri);
            if (resolvedContent.hasValue() && resolvedContent.requireValue() instanceof Content) {
                builder.add((Content)resolvedContent.requireValue());
            }
        }
        return builder.build();
    }
    

    private ImmutableSetMultimap<String, String> uriToEquivs(Iterable<LookupEntry> resolved, List<Publisher> selectedSources) {
        Predicate<LookupRef> sourceFilter = MorePredicates.transformingPredicate(LookupRef.TO_SOURCE, Predicates.in(selectedSources));

        ImmutableSetMultimap.Builder<String, String> builder = ImmutableSetMultimap.builder();
        for (LookupEntry entry : resolved) {
            Iterable<LookupRef> selectedEquivs = Iterables.filter(entry.equivalents(), sourceFilter);
            selectedEquivs = sort(selectedEquivs, selectedSources);
            builder.putAll(entry.uri(), Iterables.transform(selectedEquivs,LookupRef.TO_ID));
        }
        return builder.build();
    }

    private Iterable<LookupRef> sort(Iterable<LookupRef> selectedEquivs, List<Publisher> selectedSources) {
        return MoreOrderings.transformingOrdering(TO_SOURCE, Ordering.explicit(selectedSources)).sortedCopy(selectedEquivs);
    }

    private Iterable<LookupEntry> filter(Iterable<LookupEntry> entries, List<Publisher> selectedSources) {
        Predicate<Publisher> sourceFilter = Predicates.in(selectedSources);
        Function<LookupEntry, Publisher> toSource = Functions.compose(LookupRef.TO_SOURCE, LookupEntry.TO_SELF);
        
        return Iterables.filter(entries, MorePredicates.transformingPredicate(toSource, sourceFilter));
    }

}
