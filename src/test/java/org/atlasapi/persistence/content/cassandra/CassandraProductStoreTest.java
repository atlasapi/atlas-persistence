package org.atlasapi.persistence.content.cassandra;

import com.google.common.collect.Iterables;
import java.util.Arrays;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.product.Product;
import org.atlasapi.persistence.cassandra.BaseCassandraTest;
import org.junit.Test;
import org.junit.Before;
import static org.junit.Assert.*;
import org.junit.Ignore;

/**
 */
@Ignore(value = "Enable if running a local Cassandra instance with Atlas schema.")
public class CassandraProductStoreTest extends BaseCassandraTest {
    
    private CassandraProductStore store;
    
    @Before
    @Override
    public void before() {
        super.before();
        store = new CassandraProductStore(context, 10000);
    }
    
    @Test
    public void testProductById() {
        Product product = new Product();
        product.setId(1l);
        product.setCanonicalUri("uri1");
        product.setPublisher(Publisher.METABROADCAST);
        
        store.store(product);
        
        assertEquals(product, store.productForId(1l).get());
    }
    
    @Test
    public void testProductByUri() {
        Product product = new Product();
        product.setId(1l);
        product.setCanonicalUri("uri1");
        product.setPublisher(Publisher.METABROADCAST);
        
        store.store(product);
        
        assertEquals(product, store.productForSourceIdentified(Publisher.METABROADCAST, "uri1").get());
    }
    
    @Test
    public void testProductByContent() {
        Product product1 = new Product();
        product1.setId(1l);
        product1.setCanonicalUri("uri1");
        product1.setPublisher(Publisher.METABROADCAST);
        product1.setContent(Arrays.asList("c1", "c2"));
        Product product2 = new Product();
        product2.setId(2l);
        product2.setCanonicalUri("uri2");
        product2.setPublisher(Publisher.METABROADCAST);
        product2.setContent(Arrays.asList("c1"));
        
        store.store(product1);
        store.store(product2);
        
        assertEquals(2, Iterables.size(store.productsForContent("c1")));
        
        product1.setContent(Arrays.asList("c1"));
        product2.setContent(Arrays.asList("c2"));
        
        store.store(product1);
        store.store(product2);
        
        assertEquals(product1, Iterables.get(store.productsForContent("c1"), 0));
        assertEquals(product2, Iterables.get(store.productsForContent("c2"), 0));
    }
    
    @Test
    public void testAllProducts() {
        Product product1 = new Product();
        product1.setId(1l);
        product1.setPublisher(Publisher.METABROADCAST);
        Product product2 = new Product();
        product2.setId(2l);
        product2.setPublisher(Publisher.METABROADCAST);
        
        store.store(product1);
        store.store(product2);
        
        assertEquals(2, Iterables.size(store.products()));
    }
}
