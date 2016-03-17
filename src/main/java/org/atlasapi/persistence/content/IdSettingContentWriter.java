package org.atlasapi.persistence.content;

import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Item;

import static com.google.common.base.Preconditions.checkNotNull;

public class IdSettingContentWriter implements ContentWriter {

    private final ContentWriter delegate;
    private final LookupBackedContentIdGenerator lookupBackedContentIdGenerator;

    public IdSettingContentWriter(ContentWriter delegate,
            LookupBackedContentIdGenerator lookupBackedContentIdGenerator) {
        this.delegate = checkNotNull(delegate);
        this.lookupBackedContentIdGenerator = checkNotNull(lookupBackedContentIdGenerator);
    }
    
    @Override
    public Item createOrUpdate(Item item) {
        return delegate.createOrUpdate(ensureId(item));
    }
    
    private <T extends Content> T ensureId(T content) {
        content.setId(lookupBackedContentIdGenerator.getId(content));
        return content;
    }

    @Override
    public void createOrUpdate(Container container) {
        delegate.createOrUpdate(ensureId(container));
    }
}
