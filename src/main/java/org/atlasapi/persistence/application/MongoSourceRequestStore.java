package org.atlasapi.persistence.application;

import static org.atlasapi.persistence.application.SourceRequestTranslator.APPID_KEY;
import static org.atlasapi.persistence.application.SourceRequestTranslator.SOURCE_KEY;

import java.util.Set;

import org.atlasapi.application.SourceRequest;
import org.atlasapi.media.common.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.util.Resolved;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

import static com.metabroadcast.common.persistence.mongo.MongoBuilders.where;

public class MongoSourceRequestStore implements SourceRequestStore {

public static final String SOURCE_REQUESTS_COLLECTION = "sourceReadRequests";
    
    private final SourceRequestTranslator translator = new SourceRequestTranslator();
    
    private final DBCollection sourceRequests;
    
    private final Function<DBObject, SourceRequest> translatorFunction = new Function<DBObject, SourceRequest>(){
        @Override
        public SourceRequest apply(DBObject dbo) {
            return translator.fromDBObject(dbo);
        }
    };
    
    public MongoSourceRequestStore(DatabasedMongo mongo) {
        this.sourceRequests = mongo.collection(SOURCE_REQUESTS_COLLECTION);
    }
    @Override
    public void store(SourceRequest sourceRequest) {
        this.sourceRequests.save(translator.toDBObject(sourceRequest));
    }
    @Override
    public Optional<SourceRequest> getBy(Id applicationId, Publisher source) {
        return Optional.fromNullable(translator.fromDBObject(
                this.sourceRequests.findOne(where().fieldEquals(SOURCE_KEY, source.key())
                        .fieldEquals(APPID_KEY, applicationId.longValue()).build())));
    }
    @Override
    public Set<SourceRequest> sourceRequestsFor(Publisher source) {
        return ImmutableSet.copyOf(Iterables.transform(sourceRequests.find(
                where().fieldEquals(SOURCE_KEY, source.key()).build()), 
                translatorFunction));
    }
    @Override
    public Set<SourceRequest> all() {
        return ImmutableSet.copyOf(Iterables.transform(sourceRequests.find(), translatorFunction));
    }
    @Override
    public Optional<SourceRequest> sourceRequestFor(Id id) {
        return Optional.fromNullable(translator.fromDBObject(
                this.sourceRequests.findOne(where().idEquals(id.longValue()).build())));
    }
    @Override
    public Iterable<SourceRequest> sourceRequestsForApplicationIds(Iterable<Id> applicationIds) {
        Iterable<Long> idLongs = Iterables.transform(applicationIds, Id.toLongValue());
        return Iterables.transform(sourceRequests.find(where().longIdIn(idLongs).build())
                , translatorFunction);
    }
    
    @Override
    public ListenableFuture<Resolved<SourceRequest>> resolveIds(Iterable<Id> ids) {
        return Futures.immediateFuture(Resolved.valueOf(Iterables.transform(ids, new Function<Id, SourceRequest>() {

            @Override
            public SourceRequest apply(Id input) {
                return sourceRequestFor(input).get();
            }})));
    }
}