package org.atlasapi.persistence.content.mongo;

import static org.junit.Assert.*;

import org.atlasapi.media.common.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.product.Product;
import org.atlasapi.persistence.content.mongo.MongoProductStore;
import org.junit.BeforeClass;
import org.junit.Test;

import com.metabroadcast.common.persistence.MongoTestHelper;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;

public class MongoProductStoreTest {

    private static DatabasedMongo mongo;

    private final MongoProductStore productStore = new MongoProductStore(mongo);
    
    @BeforeClass
    public static void setUp() {
        mongo = MongoTestHelper.anEmptyTestDatabase();
    }
    
    @Test
    public void testStoresAndResolvesProduct() {
        
        Product product = new Product();
        
        product.setId(Id.valueOf(1000l));
        product.setPublisher(Publisher.BBC);
        product.setCanonicalUri("canonicalUri");
        product.setTitle("title");
        
        productStore.store(product);
        
        assertNotNull(productStore.productForId(1000l));
        assertNotNull(productStore.productForSourceIdentified(Publisher.BBC, "canonicalUri"));
    }

}
