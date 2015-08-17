package org.atlasapi.persistence.content.listing;

import com.google.common.base.Optional;

public interface ProgressStore {

    Optional<ContentListingProgress> progressForTask(String taskName);
    void storeProgress(String taskName, ContentListingProgress progress);

}
