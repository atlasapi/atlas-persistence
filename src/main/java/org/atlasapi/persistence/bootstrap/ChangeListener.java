package org.atlasapi.persistence.bootstrap;

public interface ChangeListener {

    void beforeChange();
    
    void onChange(Iterable changed);
    
    void afterChange();
}
