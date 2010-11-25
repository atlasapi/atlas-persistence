package org.atlasapi.persistence.content;

import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Playlist;

public interface DefinitiveContentWriter extends ContentWriter {
    
    void createOrUpdateDefinitiveItem(Item item);
    
    void createOrUpdateDefinitivePlaylist(Playlist playlist);
}
