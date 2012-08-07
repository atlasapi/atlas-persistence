package org.atlasapi.persistence;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.atlasapi.persistence.content.cassandra.CassandraContentStore;

/**
 */
public class CassandraContentPersistenceModule {

    private final CassandraContentStore cassandraContentStore;

    public CassandraContentPersistenceModule(String seeds, int port, int connectionTimeout, int requestTimeout) {
        this.cassandraContentStore = new CassandraContentStore(Lists.newArrayList(Splitter.on(',').split(seeds)), port, Runtime.getRuntime().availableProcessors() * 10, connectionTimeout, requestTimeout);
    }

    public CassandraContentStore cassandraContentStore() {
        return cassandraContentStore;
    }
}
