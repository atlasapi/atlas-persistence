package org.atlasapi.persistence.media;

import org.atlasapi.media.content.Container;
import org.atlasapi.media.content.Content;
import org.atlasapi.media.entity.Item;
import org.atlasapi.persistence.content.ContentHasher;
import org.atlasapi.persistence.media.entity.ContainerTranslator;
import org.atlasapi.persistence.media.entity.ItemTranslator;

import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;

public class TranslatorContentHasher implements ContentHasher {
    
    private final ItemTranslator itemTranslator;
    private final ContainerTranslator containerTranslator;

    public TranslatorContentHasher() {
        NumberToShortStringCodec codec = SubstitutionTableNumberCodec.lowerCaseOnly();
        this.itemTranslator = new ItemTranslator(codec);
        this.containerTranslator = new ContainerTranslator(codec);
    }

    @Override
    public String hash(Content content) {
        if (content instanceof Item) {
            return itemTranslator.hashCodeOf((Item)content);
        } 
        if (content instanceof Content) {
            return containerTranslator.hashCodeOf((Container)content);
        }
        throw new RuntimeException("Can't hash " + content.getClass().getSimpleName());
    }

}
