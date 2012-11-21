package org.atlasapi.persistence.content;

import org.atlasapi.media.entity.Content;

public interface ContentHasher {

    String hash(Content content);
    
}
