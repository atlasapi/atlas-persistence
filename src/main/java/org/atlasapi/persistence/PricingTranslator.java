package org.atlasapi.persistence;

import com.metabroadcast.common.currency.Price;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.atlasapi.media.entity.simple.Pricing;
import org.joda.time.DateTime;

import java.util.Currency;

public class PricingTranslator implements ModelTranslator<Pricing> {
    private static final String CURRENCY = "currency";
    private static final String PRICE = "price";
    private static final String START_TIME = "startTime";
    private static final String END_TIME = "endTime";

    @Override
    public DBObject toDBObject(DBObject dbObject, Pricing model) {
        if (dbObject == null) {
            dbObject = new BasicDBObject();
        }
        TranslatorUtils.from(dbObject, CURRENCY, model.getPrice().getCurrency().getCurrencyCode());
        TranslatorUtils.from(dbObject, PRICE, model.getPrice().getAmount());
        if (model.getStartTime() != null) {
            TranslatorUtils.fromDateTime(dbObject, START_TIME, model.getStartTime());
        }
        if(model.getEndTime() != null){
            TranslatorUtils.fromDateTime(dbObject, END_TIME, model.getEndTime());
        }
        return dbObject;
    }

    @Override
    public Pricing fromDBObject(DBObject dbObject, Pricing model) {
        Price price = new Price(
                Currency.getInstance(TranslatorUtils.toString(dbObject, CURRENCY)),
                TranslatorUtils.toInteger(dbObject, PRICE)
        );
        DateTime startTime = TranslatorUtils.toDateTime(dbObject, START_TIME);
        DateTime endTime = TranslatorUtils.toDateTime(dbObject, END_TIME);
        return new Pricing(startTime, endTime, price);
    }
}
