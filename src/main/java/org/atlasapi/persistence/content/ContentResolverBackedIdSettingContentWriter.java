package org.atlasapi.persistence.content;

import org.atlasapi.media.common.Id;
import org.atlasapi.media.content.Content;
import org.atlasapi.media.entity.Identified;

import com.google.common.collect.ImmutableList;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.ids.IdGenerator;


public class ContentResolverBackedIdSettingContentWriter extends AbstractIdSettingContentWriter {

    private final ContentResolver contentResolver;

    public ContentResolverBackedIdSettingContentWriter(ContentResolver contentResolver, IdGenerator generator, ContentWriter delegate) {
        super(generator, delegate);
        this.contentResolver = contentResolver;
    }

    @Override
    protected <T extends Content> Id getExistingId(T content) {
        ResolvedContent resolved = contentResolver.findByCanonicalUris(ImmutableList.of(content.getCanonicalUri()));
        Maybe<Identified> ided = resolved.get(content.getCanonicalUri());
        if (ided.hasValue()) {
            return ided.requireValue().getId();
        }
        return null;
    }

}
