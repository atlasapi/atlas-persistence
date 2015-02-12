package org.atlasapi.persistence.audit;

import org.atlasapi.media.entity.Described;
import org.atlasapi.persistence.lookup.entry.LookupEntry;


public interface PersistenceAuditLog {

    /**
     * Write a log entry to record the provided entity has been updated.
     * 
     * @param identified
     */
    void logWrite(Described described);
    
    /**
     * Write a log entry to record that an update was requested for the
     * provided entity but it was found to not have changed, so no write
     * need be performed.
     *  
     * @param identified
     */
    void logNoWrite(Described described);

    /**
     * Write a log entry to record the provided {@link LookupEntry} 
     * has been updated.
     * 
     * @param lookupEntry
     */
    void logWrite(LookupEntry lookupEntry);

    /**
     * Write a log entry to record that an update was requested for the
     * provided {@link LookupEntry} but it was found to not have changed, 
     * so no write need be performed.
     * 
     * @param lookupEntry
     */
    void logNoWrite(LookupEntry lookupEntry);
    
}
