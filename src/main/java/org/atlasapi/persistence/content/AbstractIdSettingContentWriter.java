package org.atlasapi.persistence.content;

import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Item;

import com.metabroadcast.common.ids.IdGenerator;

public abstract class AbstractIdSettingContentWriter implements ContentWriter {

    protected final ContentWriter delegate;
    protected final IdGenerator generator;

    public AbstractIdSettingContentWriter(IdGenerator generator, ContentWriter delegate) {
        this.generator = generator;
        this.delegate = delegate;
    }

    @Override
    public void createOrUpdate(Item item) {
        delegate.createOrUpdate(ensureId(item));
    }

    private <T extends Content> T ensureId(T content) {
        Long id = getExistingId(content);
    
        if (id == null) {
            content.setId(generator.generateRaw());
        } else { //ensures an adapter can't override and assign a new id for the content.
            content.setId(id);
        }
        
        return content;
    }

    protected abstract <T extends Content> Long getExistingId(T content);

    @Override
    public void createOrUpdate(Container container) {
        delegate.createOrUpdate(ensureId(container));
    }

}