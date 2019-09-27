package org.atlasapi.persistence.content;

import org.atlasapi.media.entity.Container;
import org.atlasapi.persistence.ApiContentFields;

public interface NullRemoveFieldsContentWriter extends ContentWriter {

    void createOrUpdate(Container container, Iterable<ApiContentFields> fieldsToRemove);
}
