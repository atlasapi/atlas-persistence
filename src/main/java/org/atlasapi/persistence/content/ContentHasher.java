package org.atlasapi.persistence.content;

import org.atlasapi.media.content.Content;

public interface ContentHasher {

    String hash(Content content);
    
}
