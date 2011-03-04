package org.atlasapi.persistence.content.mongo;

import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Item;
import org.atlasapi.persistence.content.ContentListener;

public class ScheduleUpdatingContentListener implements ContentListener {
    
    private final MongoScheduleStore store;

    public ScheduleUpdatingContentListener(MongoScheduleStore store) {
        this.store = store;
    }

    @Override
    public void brandChanged(Iterable<? extends Container<?>> containers, ChangeType changeType) {
        // ignore brand changes
    }

    @Override
    public void itemChanged(Iterable<? extends Item> items, ChangeType changeType) {
        if (changeType == ChangeType.CONTENT_UPDATE) {
            for (Item item: items) {
                store.createOrUpdate(item);
            }
        }
    }
}
