package org.atlasapi.persistence.content.mongo;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.metabroadcast.common.persistence.mongo.MongoBuilders.where;
import static com.metabroadcast.common.persistence.mongo.MongoConstants.SINGLE;
import static com.metabroadcast.common.persistence.mongo.MongoConstants.UPSERT;
import static org.atlasapi.persistence.media.entity.ProductTranslator.CONTENT_KEY;

import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.product.Product;
import org.atlasapi.persistence.media.product.ProductResolver;
import org.atlasapi.persistence.media.product.ProductStore;
import org.atlasapi.persistence.media.entity.DescribedTranslator;
import org.atlasapi.persistence.media.entity.IdentifiedTranslator;
import org.atlasapi.persistence.media.entity.ProductTranslator;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.persistence.translator.TranslatorFunction;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class MongoProductStore implements ProductResolver, ProductStore {

    private final DBCollection collection;
    private final ProductTranslator translator;
    private final Function<DBObject, Product> translatorFunction;

    public MongoProductStore(DatabasedMongo mongo) {
        this.collection = mongo.collection("products");
        this.translator = new ProductTranslator();
        this.translatorFunction = new TranslatorFunction<Product>(translator);
    }
    
    @Override
    public Product store(Product product) {
        collection.update(where().idEquals(checkNotNull(product.getId(),"Product requires id")).build(), translator.toDBObject(null, product), UPSERT, SINGLE);
        return product;
    }

    @Override
    public Optional<Product> productForId(long id) {
        return Optional.fromNullable(translator.fromDBObject(collection.findOne(id), null));
    }

    @Override
    public Optional<Product> productForSourceIdentified(Publisher source, String sourceIdentifier) {
        return Optional.fromNullable(translator.fromDBObject(collection.findOne(
            where()
                .fieldEquals(DescribedTranslator.PUBLISHER_KEY, source.key())
                .fieldEquals(IdentifiedTranslator.CANONICAL_URL, sourceIdentifier).build()
        ), null));
    }

    @Override
    public Iterable<Product> products() {
        return Iterables.transform(collection.find(), translatorFunction);
    }

    @Override
    public Iterable<Product> productsForContent(String canonicalUri) {
        return Iterables.transform(collection.find(where().fieldEquals(CONTENT_KEY, canonicalUri).build()), translatorFunction);
    }

}
