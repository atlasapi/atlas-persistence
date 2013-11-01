package org.atlasapi.persistence.content;

import java.util.List;
import java.util.Set;

import org.atlasapi.application.ApplicationConfiguration;
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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.metabroadcast.common.base.MorePredicates;

public class DefaultEquivalentContentResolver implements EquivalentContentResolver {

    private KnownTypeContentResolver contentResolver;
    private LookupEntryStore lookupResolver;

    public DefaultEquivalentContentResolver(KnownTypeContentResolver contentResolver, LookupEntryStore lookupResolver) {
        this.contentResolver = contentResolver;
        this.lookupResolver = lookupResolver;
    }
    
    @Override
    public EquivalentContent resolveUris(Iterable<String> uris, ApplicationConfiguration appConfig, Set<Annotation> activeAnnotations, boolean withAliases) {
        Iterable<LookupEntry> entries = lookupResolver.entriesForIdentifiers(uris, withAliases);
        return filterAndResolveEntries(entries, uris, appConfig.getEnabledSources());
    }
    
    @Override
    public EquivalentContent resolveIds(Iterable<Long> ids, ApplicationConfiguration appConfig, Set<Annotation> activeAnnotations) {
        Iterable<LookupEntry> entries = lookupResolver.entriesForIds(ids);
        Set<String> uris = Sets.newHashSet();
        for (LookupEntry entry : entries) {
            uris.add(entry.uri());
        }
        return filterAndResolveEntries(entries, uris, appConfig.getEnabledSources());
    }

    protected EquivalentContent filterAndResolveEntries(Iterable<LookupEntry> entries, Iterable<String> uris, Set<Publisher> selectedSources) {
        Iterable<LookupEntry> selectedEntries = filter(entries, selectedSources);
        
        if (Iterables.isEmpty(selectedEntries)) {
            return EquivalentContent.empty();
        }
        
        SetMultimap<String, LookupRef> uriToEquivs = uriToEquivs(selectedEntries, selectedSources);
        
        ResolvedContent resolvedContent = contentResolver.findByLookupRefs(uriToEquivs.values());
        
        EquivalentContent.Builder equivalentContent = EquivalentContent.builder();
        for (String uri : uris) {
            Set<LookupRef> equivUris = uriToEquivs.get(uri);
            Iterable<Content> content = equivContent(equivUris, resolvedContent);
            equivalentContent.putEquivalents(uri, content);
        }
        return equivalentContent.build();
    }

    protected Iterable<Content> equivContent(Set<LookupRef> equivUris, ResolvedContent resolved) {
        if (equivUris == null) {
            return ImmutableSet.of();
        }
        List<Identified> resolvedEquivs = resolved.getResolvedResults(Iterables.transform(equivUris,LookupRef.TO_URI));
        return Iterables.filter(resolvedEquivs, Content.class);
    }
    

    private SetMultimap<String, LookupRef> uriToEquivs(Iterable<LookupEntry> resolved, Set<Publisher> selectedSources) {
        Predicate<LookupRef> sourceFilter = MorePredicates.transformingPredicate(LookupRef.TO_SOURCE, Predicates.in(selectedSources));

        ImmutableSetMultimap.Builder<String, LookupRef> builder = ImmutableSetMultimap.builder();
        for (LookupEntry entry : resolved) {
            Set<LookupRef> selectedEquivs = Sets.filter(entry.equivalents(), sourceFilter);
            builder.putAll(entry.uri(), selectedEquivs);
        }
        return builder.build();
    }

    private Iterable<LookupEntry> filter(Iterable<LookupEntry> entries, Set<Publisher> selectedSources) {
        Predicate<Publisher> sourceFilter = Predicates.in(selectedSources);
        Function<LookupEntry, Publisher> toSource = Functions.compose(LookupRef.TO_SOURCE, LookupEntry.TO_SELF);
        
        return Iterables.filter(entries, MorePredicates.transformingPredicate(toSource, sourceFilter));
    }

}
