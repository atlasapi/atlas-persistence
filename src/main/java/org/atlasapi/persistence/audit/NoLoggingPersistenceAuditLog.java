package org.atlasapi.persistence.audit;

import org.atlasapi.media.entity.Described;
import org.atlasapi.persistence.lookup.entry.LookupEntry;


public class NoLoggingPersistenceAuditLog implements PersistenceAuditLog {

    @Override
    public void logWrite(Described described) {
        
    }

    @Override
    public void logNoWrite(Described described) {
        
    }

    @Override
    public void logWrite(LookupEntry lookupEntry) {
        
    }

    @Override
    public void logNoWrite(LookupEntry lookupEntry) {
        
    }

}
