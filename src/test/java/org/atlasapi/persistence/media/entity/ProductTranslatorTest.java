package org.atlasapi.persistence.media.entity;

import static org.junit.Assert.*;

import java.util.Currency;

import org.atlasapi.media.product.Product;
import org.atlasapi.media.product.ProductLocation;
import org.atlasapi.media.product.Product.Type;
import org.atlasapi.persistence.media.entity.ProductTranslator;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.currency.Price;
import com.mongodb.DBObject;

public class ProductTranslatorTest {

    private final ProductTranslator translator = new ProductTranslator();
    
    @Test
    public void testEncodesAndDecodesProduct() {
        
        ProductLocation location = ProductLocation.builder("locationUri")
                .withAvailability("not available")
                .withPrice(new Price(Currency.getInstance("GBP"), 1000))
                .withShippingPrice(new Price(Currency.getInstance("USD"), 150))
                .build();

        Product product = new Product();
        product.setId(1);
        product.setGtin("gtin");
        product.setType(Product.Type.BLU_RAY);
        product.setYear(3000);
        product.setContent(ImmutableList.of("contentUri"));
        product.setLocations(ImmutableList.of(location));
        
        DBObject encoded = translator.toDBObject(null, product);
        
        Product decoded = translator.fromDBObject(encoded, null);
        
        assertEquals(product.getGtin(), decoded.getGtin());
        assertEquals(product.getType(), decoded.getType());
        assertEquals(product.getYear(),decoded.getYear());
        assertEquals(product.getContent(), decoded.getContent());
        
        ProductLocation decodedLocation = Iterables.getOnlyElement(decoded.getLocations());
        
        assertEquals(location.getUri(), decodedLocation.getUri());
        assertEquals(location.getPrice(), decodedLocation.getPrice());
        assertEquals(location.getShippingPrice(), decodedLocation.getShippingPrice());
        assertEquals(location.getAvailability(), decodedLocation.getAvailability());
        
    }

}
