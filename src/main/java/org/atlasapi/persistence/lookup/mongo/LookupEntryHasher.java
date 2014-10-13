package org.atlasapi.persistence.lookup.mongo;

import static com.google.common.base.Preconditions.checkNotNull;

import org.atlasapi.persistence.lookup.entry.LookupEntry;

public class LookupEntryHasher {

    private final LookupEntryTranslator translator;
    
    public LookupEntryHasher(LookupEntryTranslator translator) {
        this.translator = checkNotNull(translator);
    }
    
    public int writeHashFor(LookupEntry lookupEntry) {
        return translator.removeFieldsForHash(translator.toDbo(lookupEntry)).hashCode();
    }
}
