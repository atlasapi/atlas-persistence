package org.atlasapi.persistence.content;

import static org.atlasapi.content.criteria.ContentQueryBuilder.query;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.atlasapi.content.criteria.ContentQuery;
import org.atlasapi.content.criteria.attribute.Attributes;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.query.KnownTypeQueryExecutor;
import org.atlasapi.persistence.content.query.UniqueContentForUriQueryExecutor;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JMock;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.google.common.collect.Lists;

@RunWith(JMock.class)
public class UniqueContentForUriQueryExecutorTest {
    private final Mockery context = new Mockery();
    private KnownTypeQueryExecutor delegate = context.mock(KnownTypeQueryExecutor.class);
    private UniqueContentForUriQueryExecutor queryExectuor = new UniqueContentForUriQueryExecutor(delegate); 

    @Test
    public void shouldRemoveDuplicateBrand() {
        final ContentQuery query = query().equalTo(Attributes.BRAND_URI, "wikipedia:glee").build();
        
        Brand brand1 = new Brand("http://www.hulu.com/glee", "hulu:glee");
        brand1.addAlias("wikipedia:glee");
        Brand brand2 = new Brand("http://channel4.com/glee", "c4:glee");
        brand2.addAlias("wikipedia:glee");
        
        final List<Brand> brands = Lists.newArrayList(brand1, brand2);
        
        context.checking(new Expectations() {{ 
            one(delegate).executeBrandQuery(query); will(returnValue(brands));
        }});
        
        List<Brand> results = queryExectuor.executeBrandQuery(query);
        assertNotNull(results);
        assertFalse(results.isEmpty());
        assertEquals(1, results.size());
        assertEquals(brand1, results.get(0));
    }
    
    @Test
    public void shouldRemoveDuplicateBrandForLocation() {
        final ContentQuery query = query().equalTo(Attributes.BRAND_URI, "wikipedia:glee").equalTo(Attributes.POLICY_AVAILABLE_COUNTRY, Lists.newArrayList("uk")).build();
        
        Brand brand1 = new Brand("http://www.hulu.com/glee", "hulu:glee");
        brand1.addAlias("wikipedia:glee");
        brand1.setPublisher(Publisher.HULU.key());
        Brand brand2 = new Brand("http://channel4.com/glee", "c4:glee");
        brand2.addAlias("wikipedia:glee");
        brand2.setPublisher(Publisher.C4.key());
        
        final List<Brand> brands = Lists.newArrayList(brand1, brand2);
        
        context.checking(new Expectations() {{ 
            one(delegate).executeBrandQuery(query); will(returnValue(brands));
        }});
        
        List<Brand> results = queryExectuor.executeBrandQuery(query);
        assertNotNull(results);
        assertFalse(results.isEmpty());
        assertEquals(1, results.size());
        assertEquals(brand2, results.get(0));
    }
    
    @Test
    public void shouldRemoveDuplicateItemForLocation() {
        final ContentQuery query = query().equalTo(Attributes.ITEM_URI, "wikipedia:glee").equalTo(Attributes.POLICY_AVAILABLE_COUNTRY, Lists.newArrayList("uk")).build();
        
        Item item1 = new Item("http://www.hulu.com/glee", "hulu:glee");
        item1.addAlias("wikipedia:glee");
        item1.setPublisher(Publisher.HULU.key());
        Item item2 = new Item("http://channel4.com/glee", "c4:glee");
        item2.addAlias("wikipedia:glee");
        item2.setPublisher(Publisher.C4.key());
        
        final List<Item> items = Lists.newArrayList(item1, item2);
        
        context.checking(new Expectations() {{ 
            one(delegate).executeItemQuery(query); will(returnValue(items));
        }});
        
        List<Item> results = queryExectuor.executeItemQuery(query);
        assertNotNull(results);
        assertFalse(results.isEmpty());
        assertEquals(1, results.size());
        assertEquals(item2, results.get(0));
    }
    
    @Test
    public void shouldWorkWithNoResults() {
        final ContentQuery query = query().equalTo(Attributes.BRAND_URI, "wikipedia:glee").build();
        
        final List<Brand> brands = Lists.newArrayList();
        
        context.checking(new Expectations() {{ 
            one(delegate).executeBrandQuery(query); will(returnValue(brands));
        }});
        
        List<Brand> results = queryExectuor.executeBrandQuery(query);
        assertNotNull(results);
        assertTrue(results.isEmpty());
    }
    
    @Test
    public void shouldWorkReturnJustOne() {
        final ContentQuery query = query().equalTo(Attributes.BRAND_URI, "wikipedia:glee").build();
        
        Brand brand1 = new Brand("http://www.hulu.com/glee", "hulu:glee");
        brand1.addAlias("wikipedia:glee");
        
        final List<Brand> brands = Lists.newArrayList(brand1);
        
        context.checking(new Expectations() {{ 
            one(delegate).executeBrandQuery(query); will(returnValue(brands));
        }});
        
        List<Brand> results = queryExectuor.executeBrandQuery(query);
        assertNotNull(results);
        assertFalse(results.isEmpty());
        assertEquals(1, results.size());
        assertEquals(brand1, results.get(0));
    }
}
