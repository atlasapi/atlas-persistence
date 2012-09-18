package org.atlasapi.persistence;

import org.atlasapi.persistence.bootstrap.ContentBootstrapper;
import org.atlasapi.persistence.content.cassandra.CassandraContentStore;
import org.atlasapi.persistence.content.mongo.MongoContentLister;

/**
 */
public class ContentBootstrapperModule {

    private final MongoContentLister mongoContentStore;
    private final CassandraContentStore cassandraContentStore;
    private final ContentBootstrapper esContentBootstrapper;

    public ContentBootstrapperModule(MongoContentLister mongoContentStore, CassandraContentStore cassandraContentStore) {
        this.mongoContentStore = mongoContentStore;
        this.cassandraContentStore = cassandraContentStore;
        this.esContentBootstrapper = new ContentBootstrapper().withContentListers(mongoContentStore, cassandraContentStore);
    }

    public ContentBootstrapper contentBootstrapper() {
        return esContentBootstrapper;
    }
}
