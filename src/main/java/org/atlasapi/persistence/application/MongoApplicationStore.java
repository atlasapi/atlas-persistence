package org.atlasapi.persistence.application;

import static com.metabroadcast.common.persistence.mongo.MongoBuilders.where;
import static org.atlasapi.persistence.application.ApplicationSourcesTranslator.PUBLISHER_KEY;
import static org.atlasapi.persistence.application.ApplicationSourcesTranslator.SOURCES_KEY;
import static org.atlasapi.persistence.application.ApplicationSourcesTranslator.STATE_KEY;
import static org.atlasapi.persistence.application.ApplicationSourcesTranslator.WRITABLE_KEY;
import static org.atlasapi.persistence.application.MongoApplicationTranslator.CONFIG_KEY;
import static org.atlasapi.persistence.application.MongoApplicationTranslator.DEER_ID_KEY;

import org.atlasapi.application.Application;
import org.atlasapi.application.SourceStatus.SourceState;
import org.atlasapi.media.common.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.util.Resolved;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.ids.IdGenerator;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.text.MoreStrings;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.ReadPreference;

public class MongoApplicationStore extends AbstractApplicationStore implements ApplicationStore {

    public static final String APPLICATION_COLLECTION = "applications";
    private final DBCollection applications;
    private final MongoApplicationTranslator translator = new MongoApplicationTranslator();

    private final Function<DBObject, Application> translatorFunction = new Function<DBObject, Application>() {

        @Override
        public Application apply(DBObject dbo) {
            return translator.fromDBObject(dbo);
        }
    };

    public MongoApplicationStore(IdGenerator idGenerator, 
            NumberToShortStringCodec idCodec,
            DatabasedMongo adminMongo) {
        super(idGenerator, idCodec);
        this.applications = adminMongo.collection(APPLICATION_COLLECTION);
        this.applications.setReadPreference(ReadPreference.primary());
    }

    @Override
    public Iterable<Application> allApplications() {
        return Iterables.transform(applications.find(where().build()), translatorFunction);
    }

    @Override
    public Optional<Application> applicationFor(Id id) {
        return Optional.fromNullable(translator.fromDBObject(
                applications.findOne(
                        where().fieldEquals(DEER_ID_KEY, id.longValue())
                                .build())
                )
                );
    }

    private void store(Application application) {
        applications.save(translator.toDBObject(application));
    }

    @Override
    public Iterable<Application> applicationsFor(Iterable<Id> ids) {
        Iterable<Long> idLongs = Iterables.transform(ids, Id.toLongValue());
        return Iterables.transform(applications.find(where()
                .longFieldIn(MongoApplicationTranslator.DEER_ID_KEY,idLongs).build()), translatorFunction);
    }

    @Override
    public Iterable<Application> readersFor(Publisher source) {
        String sourceField = String.format("%s.%s.%s", CONFIG_KEY, SOURCES_KEY, PUBLISHER_KEY);
        String stateField =  String.format("%s.%s.%s", CONFIG_KEY, SOURCES_KEY, STATE_KEY);
        return ImmutableSet.copyOf(Iterables.transform(applications.find(where().fieldEquals(sourceField, source.key()).fieldIn(stateField, states()).build()), translatorFunction)); 
    }

    @Override
    public Iterable<Application> writersFor(Publisher source) {
        String sourceField = String.format("%s.%s", CONFIG_KEY, WRITABLE_KEY);
        return ImmutableSet.copyOf(Iterables.transform(applications.find(where().fieldEquals(sourceField, source.key()).build()), translatorFunction));

     }
    
    private Iterable<String> states() {
        return Iterables.transform(ImmutableSet.of(SourceState.AVAILABLE, SourceState.REQUESTED), Functions.compose(MoreStrings.toLower(), Functions.toStringFunction()));
    }

    @Override
    public void doCreateApplication(Application application) {
        store(application);
    }

    @Override
    public void doUpdateApplication(Application application) {
        store(application);
    }

    @Override
    public ListenableFuture<Resolved<Application>> resolveIds(Iterable<Id> ids) {
        return Futures.immediateFuture(Resolved.valueOf(Iterables.transform(ids, new Function<Id, Application>() {

            @Override
            public Application apply(Id input) {
                return applicationFor(input).get();
            }})));
    }

    
    
  
}
