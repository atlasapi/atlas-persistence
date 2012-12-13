package org.atlasapi.persistence;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.metabroadcast.common.properties.Configurer;

import org.atlasapi.equiv.CassandraEquivalenceSummaryStore;
import org.atlasapi.persistence.content.cassandra.CassandraContentStore;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 */
@Configuration
public class CassandraPersistenceModule {

    private final String environment = Configurer.get("cassandra.env").get();
    private final String seeds = Configurer.get("cassandra.seeds").get();
    private final String port = Configurer.get("cassandra.port").get();
    private final String connectionTimeout = Configurer.get("cassandra.connectionTimeout").get();
    private final String requestTimeout = Configurer.get("cassandra.requestTimeout").get();

    @Bean
    @Qualifier(value = "cassandra")
    public CassandraContentStore cassandraContentStore() {
        return new CassandraContentStore(environment, Lists.newArrayList(Splitter.on(',').split(seeds)), Integer.parseInt(port), Runtime.getRuntime().availableProcessors() * 10, Integer.parseInt(connectionTimeout), Integer.parseInt(requestTimeout));
    }
    
    @Bean
    @Qualifier(value = "cassandra")
    public CassandraEquivalenceSummaryStore cassandraEquivalenceSummaryStore() {
        return new CassandraEquivalenceSummaryStore(environment, Lists.newArrayList(Splitter.on(',').split(seeds)), Integer.parseInt(port), Runtime.getRuntime().availableProcessors() * 10, Integer.parseInt(connectionTimeout), Integer.parseInt(requestTimeout));
    }
}
