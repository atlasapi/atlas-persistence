package org.atlasapi.remotesite.amazon.indexer;

public interface AmazonTitleIndexStore {

     AmazonTitleIndexEntry createOrUpdateIndex(AmazonTitleIndexEntry amazonTitleIndexEntry);

     AmazonTitleIndexEntry getIndexEntry(String title);
}
