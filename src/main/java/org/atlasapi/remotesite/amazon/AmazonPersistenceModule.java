package org.atlasapi.remotesite.amazon;

import com.google.common.annotations.VisibleForTesting;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.properties.Configurer;
import com.metabroadcast.common.properties.Parameter;
import com.mongodb.Mongo;
import org.atlasapi.remotesite.amazon.indexer.AmazonTitleIndexStore;
import org.atlasapi.remotesite.amazon.indexer.AmazonTitleIndexStoreImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
public class AmazonPersistenceModule {

    @Autowired private Mongo mongo;

    private final Parameter processingConfig = Configurer.get("processing.config");
    @Value("${mongo.amazon.dbname}") private String amazonDbName;

    public AmazonPersistenceModule() {}

    @VisibleForTesting
    public AmazonPersistenceModule(
            Mongo mongo,
            String amazonDbName
            ) {
        this.mongo = mongo;
        this.amazonDbName = amazonDbName;
    }


    private transient AmazonTitleIndexStore amazonTitleIndexStore;
    @Primary
    @Bean
    public AmazonTitleIndexStore amazonTitleIndexStore() {
        if(amazonTitleIndexStore == null) {
            amazonTitleIndexStore = new AmazonTitleIndexStoreImpl(new DatabasedMongo(mongo, amazonDbName));
        }
        return amazonTitleIndexStore;
    }

}
