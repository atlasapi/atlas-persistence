package org.atlasapi.media.product;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Currency;
import java.util.List;

import org.atlasapi.persistence.media.entity.DescribedTranslator;
import org.atlasapi.persistence.media.entity.IdentifiedTranslator;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.metabroadcast.common.currency.Price;
import com.metabroadcast.common.persistence.translator.ModelTranslator;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class ProductTranslator implements ModelTranslator<Product> {

    
    private static final String GTIN_KEY = "gtin";
    private static final String TYPE_KEY = "type";
    private static final String YEAR_KEY = "year";
    public static final String CONTENT_KEY = "content";
    private static final String LOCATIONS_KEY = "locations";
    private static final String URI_KEY = "uri";
    private static final String AVAILABILITY_KEY = "availability";
    private static final String PRICE_KEY = "price";
    private static final String SHIPPING_PRICE_KEY = "shippingPrice";
    private static final String CURRENCY_KEY = "currency";
    private static final String AMOUNT_KEY = "amount";
    
    private DescribedTranslator describedTranslator;

    public ProductTranslator() {
        describedTranslator = new DescribedTranslator(new IdentifiedTranslator(true));
    }
    
    @Override
    public DBObject toDBObject(DBObject dbo, Product model) {
        checkNotNull(model);
        
        dbo = dbo == null ? new BasicDBObject() : dbo;

        describedTranslator.toDBObject(dbo, model);
        
        TranslatorUtils.from(dbo, GTIN_KEY, model.getGtin());
        TranslatorUtils.from(dbo, TYPE_KEY, model.getType() != null ? model.getType().toString() : null);
        TranslatorUtils.from(dbo, YEAR_KEY, model.getYear());
        TranslatorUtils.fromList(dbo, model.getContent(), CONTENT_KEY);
        
        dbo.put(LOCATIONS_KEY, toDBObject(model.getLocations()));
        
        return dbo;
    }

    private BasicDBList toDBObject(Iterable<ProductLocation> locations) {
        BasicDBList locationDbos = new BasicDBList();
        for (ProductLocation location : locations) {
            locationDbos.add(toDBObject(location));
        }
        return locationDbos;
    }

    private DBObject toDBObject(ProductLocation location) {
        BasicDBObject dbo = new BasicDBObject();
        TranslatorUtils.from(dbo, URI_KEY, location.getUri());
        TranslatorUtils.from(dbo, AVAILABILITY_KEY, location.getAvailability());
        TranslatorUtils.from(dbo, PRICE_KEY, toDbo(location.getPrice()));
        TranslatorUtils.from(dbo, SHIPPING_PRICE_KEY, toDbo(location.getShippingPrice()));
        return dbo;
    }

    private DBObject toDbo(Price price) {
        BasicDBObject dbo = new BasicDBObject();
        dbo.put(CURRENCY_KEY, price.getCurrency().getCurrencyCode());
        dbo.put(AMOUNT_KEY, price.getAmount());
        return dbo;
    }

    @Override
    public Product fromDBObject(DBObject dbo, Product model) {
        
        model = model == null ? new Product() : model;
        
        describedTranslator.fromDBObject(dbo, model);
        
        model.setGtin(TranslatorUtils.toString(dbo, GTIN_KEY));
        model.setType(Product.Type.fromString(TranslatorUtils.toString(dbo, TYPE_KEY)).orNull());
        model.setYear(TranslatorUtils.toInteger(dbo, YEAR_KEY));
        model.setContent(TranslatorUtils.toList(dbo, CONTENT_KEY));
        model.setLocations(fromDBObject(TranslatorUtils.toDBObjectList(dbo, LOCATIONS_KEY)));
        
        return model;
    }

    private Iterable<ProductLocation> fromDBObject(List<DBObject> dbObjectList) {
        return Lists.transform(dbObjectList, new Function<DBObject, ProductLocation>() {
            @Override
            public ProductLocation apply(DBObject input) {
                return ProductLocation.builder(TranslatorUtils.toString(input, URI_KEY))
                        .withAvailability(TranslatorUtils.toString(input, AVAILABILITY_KEY))
                        .withPrice(fromDBObject(TranslatorUtils.toDBObject(input, PRICE_KEY)))
                        .withShippingPrice(fromDBObject(TranslatorUtils.toDBObject(input, SHIPPING_PRICE_KEY)))
                        .build();
            }
        });
    }

    private Price fromDBObject(DBObject dbObject) {
        return new Price(Currency.getInstance(TranslatorUtils.toString(dbObject, CURRENCY_KEY)), TranslatorUtils.toInteger(dbObject, AMOUNT_KEY));
    }
}
