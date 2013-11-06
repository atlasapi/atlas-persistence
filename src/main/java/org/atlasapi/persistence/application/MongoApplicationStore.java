package org.atlasapi.persistence.application;

import static com.metabroadcast.common.persistence.mongo.MongoBuilders.where;
import static org.atlasapi.persistence.application.MongoApplicationTranslator.APPLICATION_ID_KEY;
import static org.atlasapi.persistence.application.ApplicationSourcesTranslator.PUBLISHER_KEY;
import static org.atlasapi.persistence.application.ApplicationSourcesTranslator.SOURCES_KEY;
import static org.atlasapi.persistence.application.ApplicationSourcesTranslator.STATE_KEY;
import static org.atlasapi.persistence.application.ApplicationSourcesTranslator.WRITABLE_KEY;
import static org.atlasapi.persistence.application.MongoApplicationTranslator.CONFIG_KEY;
import static com.metabroadcast.common.persistence.mongo.MongoConstants.NO_UPSERT;
import static com.metabroadcast.common.persistence.mongo.MongoConstants.SINGLE;

import java.util.Map;
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
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.ids.IdGenerator;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.text.MoreStrings;
import com.mongodb.CommandResult;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.ReadPreference;

public class MongoApplicationStore extends AbstractApplicationStore implements LegacyApplicationStore {

    public static final String APPLICATION_COLLECTION = "applications";
    private static final int MONGODB_DUPLICATE_KEY_ERROR = 11000;
    private static final String CREDENTIALS_API_KEY = String.format("%s.%s", 
            MongoApplicationTranslator.CREDENTIALS_KEY, 
            ApplicationCredentialsTranslator.API_KEY_KEY);
    private final DBCollection applications;
    private final DatabasedMongo adminMongo;
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
        this.adminMongo = adminMongo;
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
                        where().fieldEquals(APPLICATION_ID_KEY, id.longValue())
                                .build())
                )
                );
    }

    @Override
    public Iterable<Application> applicationsFor(Iterable<Id> ids) {
        Iterable<Long> idLongs = Iterables.transform(ids, Id.toLongValue());
        return Iterables.transform(applications.find(where()
                .longFieldIn(MongoApplicationTranslator.APPLICATION_ID_KEY,idLongs).build()), translatorFunction);
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
        applications.insert(translator.toDBObject(application));
        CommandResult result = adminMongo.database().getLastError();
        if (result.get("err") != null && result.getInt("code") == MONGODB_DUPLICATE_KEY_ERROR) {
            throw new IllegalArgumentException("Duplicate application slug");
        }
    }

    @Override
    public void doUpdateApplication(Application application) {
        // check slug correct for this deer id
        applications.update(where().idEquals(application.getSlug()).fieldEquals(APPLICATION_ID_KEY, application.getId().longValue()).build(), translator.toDBObject(application), NO_UPSERT, SINGLE);
    }

    @Override
    public ListenableFuture<Resolved<Application>> resolveIds(Iterable<Id> ids) {
        return Futures.immediateFuture(Resolved.valueOf(Iterables.transform(ids, new Function<Id, Application>() {

            @Override
            public Application apply(Id input) {
                return applicationFor(input).get();
            }})));
    }

    @Override
    public Optional<Application> applicationForKey(String apiKey) {
        return Optional.fromNullable(translator.fromDBObject(
                applications.findOne(
                        where().fieldEquals(CREDENTIALS_API_KEY, apiKey).build())
                )
         );
    }

    @Override
    @Deprecated
    public Optional<Id> applicationIdForSlug(String slug) {
        Optional<Application> application =  Optional.fromNullable(translator.fromDBObject(
                applications.findOne(
                        where().idEquals(slug).build())
                )
         );
        if (application.isPresent()) {
            return Optional.of(application.get().getId());
        } else {
            return Optional.absent();
        }
    }
    
    @Override
    @Deprecated
    public Iterable<Id> applicationIdsForSlugs(Iterable<String> slugs) {
        return Iterables.transform(slugs, new Function<String, Id>() {
            @Override
            public Id apply(String input) {
                return applicationIdForSlug(input).get();
            }
        });
    }
}
