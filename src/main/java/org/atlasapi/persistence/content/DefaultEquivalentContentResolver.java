package org.atlasapi.persistence.content;

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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.metabroadcast.common.base.MorePredicates;

public class DefaultEquivalentContentResolver implements EquivalentContentResolver {

    private ContentResolver contentResolver;
    private LookupEntryStore lookupResolver;

    public DefaultEquivalentContentResolver(ContentResolver contentResolver, LookupEntryStore lookupResolver) {
        this.contentResolver = contentResolver;
        this.lookupResolver = lookupResolver;
    }
    
    @Override
    public EquivalentContent resolveUris(Iterable<String> uris, Set<Publisher> selectedSources, Set<Annotation> activeAnnotations, boolean withAliases) {
        Iterable<LookupEntry> entries = lookupResolver.entriesForIdentifiers(uris, withAliases);
        Set<Long> ids = Sets.newHashSet();
        for (LookupEntry entry : entries) {
            ids.add(entry.id());
        }
        return filterAndResolveEntries(entries, ids, selectedSources);
    }
    
    @Override
    public EquivalentContent resolveIds(Iterable<Long> ids, Set<Publisher> selectedSources, Set<Annotation> activeAnnotations) {
        Iterable<LookupEntry> entries = lookupResolver.entriesForIds(ids);
        Set<String> uris = Sets.newHashSet();
        for (LookupEntry entry : entries) {
            uris.add(entry.uri());
        }
        return filterAndResolveEntries(entries, ImmutableSet.copyOf(ids), selectedSources);
    }

    protected EquivalentContent filterAndResolveEntries(Iterable<LookupEntry> entries, Iterable<Long> ids, Set<Publisher> selectedSources) {
        Iterable<LookupEntry> selectedEntries = filter(entries, selectedSources);
        
        if (Iterables.isEmpty(selectedEntries)) {
            return EquivalentContent.empty();
        }
        
        SetMultimap<Long, Long> idToEquivs = idToEquivs(selectedEntries, selectedSources);
        
        ResolvedContent resolvedContent = contentResolver.findByIds(idToEquivs.values());
        
        EquivalentContent.Builder equivalentContent = EquivalentContent.builder();
        for (Long id : ids) {
            Set<Long> equivUris = idToEquivs.get(id);
            Iterable<Content> content = equivContent(equivUris, resolvedContent);
            equivalentContent.putEquivalents(id, content);
        }
        return equivalentContent.build();
    }

    protected Iterable<Content> equivContent(Set<Long> equivIds, ResolvedContent resolved) {
        if (equivIds == null) {
            return ImmutableSet.of();
        }
        ImmutableMap<Long, Identified> idIndex = Maps.uniqueIndex(resolved.getAllResolvedResults(), Identified.TO_ID);
        return Iterables.filter(Iterables.transform(equivIds, Functions.forMap(idIndex, null)), Content.class);
    }
    

    private SetMultimap<Long, Long> idToEquivs(Iterable<LookupEntry> resolved, Set<Publisher> selectedSources) {
        Predicate<LookupRef> sourceFilter = MorePredicates.transformingPredicate(LookupRef.TO_SOURCE, Predicates.in(selectedSources));

        ImmutableSetMultimap.Builder<Long, Long> builder = ImmutableSetMultimap.builder();
        for (LookupEntry entry : resolved) {
            Set<LookupRef> selectedEquivs = Sets.filter(entry.equivalents(), sourceFilter);
            builder.putAll(entry.id(), Iterables.transform(selectedEquivs,LookupRef.TO_ID));
        }
        return builder.build();
    }

    private Iterable<LookupEntry> filter(Iterable<LookupEntry> entries, Set<Publisher> selectedSources) {
        Predicate<Publisher> sourceFilter = Predicates.in(selectedSources);
        Function<LookupEntry, Publisher> toSource = Functions.compose(LookupRef.TO_SOURCE, LookupEntry.TO_SELF);
        
        return Iterables.filter(entries, MorePredicates.transformingPredicate(toSource, sourceFilter));
    }

}
