package org.atlasapi.persistence.content;

import org.atlasapi.media.entity.Container;

public interface NullRemoveFieldsContentWriter extends ContentWriter {

    void createOrUpdate(Container container, boolean nullRemoveFields);

}
