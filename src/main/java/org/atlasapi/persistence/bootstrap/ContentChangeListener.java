package org.atlasapi.persistence.bootstrap;

import org.atlasapi.media.entity.Described;

public interface ContentChangeListener {

    void beforeContentChange();
    
    void contentChange(Iterable<? extends Described> content);
    
    void afterContentChange();
}
