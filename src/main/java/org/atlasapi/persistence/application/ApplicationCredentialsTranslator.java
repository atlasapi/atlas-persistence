package org.atlasapi.persistence.application;

import org.atlasapi.application.ApplicationCredentials;

import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class ApplicationCredentialsTranslator {
    public static final String API_KEY_KEY = "api_key";

    public DBObject toDBObject(ApplicationCredentials credentials) {
        DBObject dbo = new BasicDBObject();

        TranslatorUtils.from(dbo, API_KEY_KEY, credentials.getApiKey());
        return dbo;
    }

    public ApplicationCredentials fromDBObject(DBObject dbo) {
        String apiKey = TranslatorUtils.toString(dbo, API_KEY_KEY);
        return ApplicationCredentials.builder().withApiKey(apiKey).build();
    }
}
