package org.atlasapi.persistence.content.mongo;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.metabroadcast.common.persistence.mongo.MongoBuilders.where;
import static com.metabroadcast.common.persistence.mongo.MongoConstants.ID;
import static com.metabroadcast.common.persistence.mongo.MongoConstants.SINGLE;
import static com.metabroadcast.common.persistence.mongo.MongoConstants.UPSERT;

import java.net.UnknownHostException;

import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Service;
import org.atlasapi.media.entity.Topic;
import org.atlasapi.persistence.media.entity.DescribedTranslator;
import org.atlasapi.persistence.media.entity.IdentifiedTranslator;
import org.atlasapi.persistence.media.entity.ImageTranslator;
import org.atlasapi.persistence.service.ServiceResolver;

import com.google.common.base.Optional;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.persistence.mongo.MongoConstants;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;


public class MongoServiceStore implements ServiceResolver {

    private static final String COLLECTION = "services";
    
    private final DBCollection collection;

    private DescribedTranslator serviceTranslator;

    public MongoServiceStore(DatabasedMongo mongo) {
        this.collection = mongo.collection(COLLECTION);
        this.serviceTranslator = new DescribedTranslator(new IdentifiedTranslator(true), new ImageTranslator());
    }
    @Override
    public Optional<Service> serviceFor(long id) {
        DBObject dbo = collection.findOne(where().idEquals(id).build());
        if (dbo == null) {
            return Optional.absent();
        }
        return Optional.of((Service) serviceTranslator.fromDBObject(dbo, new Service()));
    }

    @Override
    public Iterable<Service> servicesFor(Alias alias) {
        throw new UnsupportedOperationException();
    }
    
    // TODO: flesh this out and create a separate translator for this type
    public void write(Service service) {
        checkNotNull(service.getId(), "Can't persist topic with no ID");

        DBObject dbo = serviceTranslator.toDBObject(new BasicDBObject(), service);
        collection.update(where().idEquals((Long)dbo.get(ID)).build(), dbo, UPSERT, SINGLE);
    }
    
    public static void main(String[] args) throws UnknownHostException {
        MongoServiceStore store = new MongoServiceStore(new DatabasedMongo(new MongoClient(), "atlas"));
        Service service = new Service();
        service.setId(1234L);
        service.setCanonicalUri("http://atlas.metabroadcast.com/platforms/youview/");
        service.setTitle("YouView");
        store.write(service);
    }

}
