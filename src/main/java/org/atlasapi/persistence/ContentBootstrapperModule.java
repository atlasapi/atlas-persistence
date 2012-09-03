package org.atlasapi.persistence;

import org.atlasapi.persistence.bootstrap.ContentBootstrapper;
import org.atlasapi.persistence.content.cassandra.CassandraContentStore;

/**
 */
public class ContentBootstrapperModule {

    private final CassandraContentStore cassandraContentStore;
    private final ContentBootstrapper esContentBootstrapper;

    public ContentBootstrapperModule(CassandraContentStore cassandraContentStore) {
        this.cassandraContentStore = cassandraContentStore;
        this.esContentBootstrapper = new ContentBootstrapper().withContentListers(cassandraContentStore);
    }

    public ContentBootstrapper contentBootstrapper() {
        return esContentBootstrapper;
    }
}
