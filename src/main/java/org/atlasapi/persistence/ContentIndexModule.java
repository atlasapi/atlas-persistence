package org.atlasapi.persistence;

import org.atlasapi.persistence.content.ContentIndexer;
import org.atlasapi.persistence.content.schedule.ScheduleIndex;

/**
 */
public interface ContentIndexModule {
    
    void init();
    
    ContentIndexer contentIndexer();

    ScheduleIndex scheduleIndex();
    
}
