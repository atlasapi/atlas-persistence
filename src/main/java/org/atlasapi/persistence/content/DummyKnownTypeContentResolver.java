package org.atlasapi.persistence.content;

import java.util.Map;

import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.persistence.content.ResolvedContent.ResolvedContentBuilder;

import com.google.common.collect.Maps;

public class DummyKnownTypeContentResolver implements KnownTypeContentResolver {
    
    private final Map<String, Identified> content = Maps.newHashMap();
    
    @Override
    public ResolvedContent findByLookupRefs(Iterable<LookupRef> lookupRefs) {
        ResolvedContentBuilder results = new ResolvedContentBuilder();
        
        for (LookupRef ref : lookupRefs) {
            results.put(ref.uri(), content.get(ref.uri()));
        }
        
        return results.build();
    }
    
    public DummyKnownTypeContentResolver respondTo(Iterable<? extends Identified> content) {
        for (Identified item : content) {
            this.content.put(item.getCanonicalUri(), item);
        }
        return this;
    }
}
