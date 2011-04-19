package org.atlasapi.persistence.content.schedule.mongo;

import org.atlasapi.media.entity.Item;

public interface ScheduleWriter {

    void writeScheduleFor(Iterable<? extends Item> items);

}
