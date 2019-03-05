package org.atlasapi.remotesite.amazon;

import org.atlasapi.remotesite.amazon.indexer.AmazonTitleIndexStore;

public interface AmazonPersistenceModuleInterface {

    AmazonTitleIndexStore amazonTitleIndexStore();
}
