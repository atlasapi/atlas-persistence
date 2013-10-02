package org.atlasapi.persistence.application;

import org.atlasapi.application.Application;
import org.atlasapi.media.common.Id;

import com.metabroadcast.common.persistence.mongo.MongoConstants;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class MongoApplicationTranslator {

    public static final String SLUG_KEY = MongoConstants.ID;
    public static final String APPLICATION_ID_KEY = "aid";
    public static final String TITLE_KEY = "title";
    public static final String DESCRIPTION_KEY = "description";
    public static final String CREATED_KEY = "created";
    public static final String CREDENTIALS_KEY = "credentials";
    public static final String CONFIG_KEY = "configuration";

    private final ApplicationCredentialsTranslator credentialsTranslator = new ApplicationCredentialsTranslator();
    private final ApplicationSourcesTranslator sourcesTranslator = new ApplicationSourcesTranslator();

    public DBObject toDBObject(Application application) {
        DBObject dbo = new BasicDBObject();
        TranslatorUtils.from(dbo, APPLICATION_ID_KEY, application.getId().longValue());
        TranslatorUtils.from(dbo, SLUG_KEY, application.getSlug());
        TranslatorUtils.from(dbo, TITLE_KEY, application.getTitle());
        TranslatorUtils.from(dbo,  DESCRIPTION_KEY, application.getDescription());
        TranslatorUtils.fromDateTime(dbo, CREATED_KEY, application.getCreated());
        TranslatorUtils.from(dbo,
                CREDENTIALS_KEY,
                credentialsTranslator.toDBObject(application.getCredentials()));
        TranslatorUtils.from(dbo,
                CONFIG_KEY,
                sourcesTranslator.toDBObject(application.getSources()));
        return dbo;
    }

    public Application fromDBObject(DBObject dbo) {
        if (dbo == null) {
            return null;
        }

        return Application.builder()
                .withId(Id.valueOf(TranslatorUtils.toLong(dbo, APPLICATION_ID_KEY)))
                .withSlug(TranslatorUtils.toString(dbo, SLUG_KEY))
                .withTitle(TranslatorUtils.toString(dbo, TITLE_KEY))
                .withDescription(TranslatorUtils.toString(dbo, DESCRIPTION_KEY))
                .withCreated(TranslatorUtils.toDateTime(dbo, CREATED_KEY))
                .withCredentials(credentialsTranslator.fromDBObject(TranslatorUtils.toDBObject(dbo,
                        CREDENTIALS_KEY)))
                .withSources(sourcesTranslator.fromDBObject(TranslatorUtils.toDBObject(dbo,
                        CONFIG_KEY)))
                .build();
    }
}
