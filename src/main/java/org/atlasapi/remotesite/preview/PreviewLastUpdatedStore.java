package org.atlasapi.remotesite.preview;

import com.google.common.base.Optional;

public interface PreviewLastUpdatedStore {

    void store(String lastUpdated);

    Optional<String> retrieve();
}
