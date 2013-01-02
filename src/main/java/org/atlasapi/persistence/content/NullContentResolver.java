package org.atlasapi.persistence.content;

import org.atlasapi.persistence.content.ResolvedContent.ResolvedContentBuilder;

public class NullContentResolver implements ContentResolver {

    private static final ContentResolver INSTANCE = new NullContentResolver();
    private static final ResolvedContent EMPTY = new ResolvedContentBuilder().build();
    
    public static final ContentResolver get() {
        return INSTANCE;
    }
    
    private NullContentResolver() {
    }
    
    @Override
    public ResolvedContent findByCanonicalUris(Iterable<String> canonicalUris) {
        return EMPTY;
    }

}
