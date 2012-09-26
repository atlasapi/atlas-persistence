package org.atlasapi.persistence.bootstrap;

import org.atlasapi.media.entity.Identified;

public interface ChangeListener {

    void beforeChange();
    
    void onChange(Iterable<? extends Identified> changed);
    
    void afterChange();
}
