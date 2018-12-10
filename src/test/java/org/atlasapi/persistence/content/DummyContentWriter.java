package org.atlasapi.persistence.content;

import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Item;

public class DummyContentWriter implements ContentWriter {

    @Override
    public Item createOrUpdate(Item item) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createOrUpdate(Container container) {
        throw new UnsupportedOperationException();
    }
}
