package org.uriplay.persistence.content;

import static org.junit.Assert.*;
import static org.uriplay.content.criteria.ContentQueryBuilder.query;

import java.util.List;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JMock;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.uriplay.content.criteria.ContentQuery;
import org.uriplay.content.criteria.attribute.Attributes;
import org.uriplay.media.entity.Brand;
import org.uriplay.persistence.content.query.KnownTypeQueryExecutor;
import org.uriplay.persistence.content.query.UniqueContentForUriQueryExecutor;

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
