package org.atlasapi.remotesite.preview;

public interface PreviewLastUpdatedStore {

    void store(String lastUpdated);

    String retrieve();
}
