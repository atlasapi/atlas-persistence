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

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.text.MoreStrings;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.ReadPreference;

public class MongoApplicationStore implements ApplicationStore {

    public static final String APPLICATION_COLLECTION = "applications";
    private final DBCollection applications;
    private final MongoApplicationTranslator translator = new MongoApplicationTranslator();

    private final Function<DBObject, Application> translatorFunction = new Function<DBObject, Application>() {

        @Override
        public Application apply(DBObject dbo) {
            return translator.fromDBObject(dbo);
        }
    };
    
    private final Function<Id, Long> idToLongTransformer = new Function<Id, Long>() {

        @Override
        public Long apply(Id input) {
            // TODO Auto-generated method stub
            return input.longValue();
        }};

    public MongoApplicationStore(DatabasedMongo adminMongo) {
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

    @Override
    public void store(Application application) {
        Preconditions.checkNotNull(application);
        applications.save(translator.toDBObject(application));
    }

    @Override
    public Iterable<Application> applicationsFor(Iterable<Id> ids) {
        Iterable<Long> idLongs = Iterables.transform(ids, idToLongTransformer);
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
}
