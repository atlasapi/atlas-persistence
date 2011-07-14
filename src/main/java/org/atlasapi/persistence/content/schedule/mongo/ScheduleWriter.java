package org.atlasapi.persistence.content.schedule.mongo;

import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.ScheduleEntry;

public interface ScheduleWriter {

    @Deprecated
    void writeScheduleFor(Iterable<? extends Item> items);

    void writeCompleteEntry(ScheduleEntry entry);

}
