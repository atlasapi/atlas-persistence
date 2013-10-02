package org.atlasapi.persistence.application;


import org.atlasapi.application.SourceRequest;
import org.atlasapi.application.UsageType;
import org.atlasapi.media.common.Id;
import org.atlasapi.media.entity.Publisher;
import com.metabroadcast.common.persistence.mongo.MongoConstants;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;


public class SourceRequestTranslator {
    public static final String APPID_KEY = "appId";
    public static final String SOURCE_KEY = "publisher";
    public static final String USAGE_TYPE_KEY = "usageType";
    public static final String EMAIL_KEY = "email";
    public static final String APPURL_KEY = "appUrl";
    public static final String REASON_KEY = "reason";
    public static final String APPROVED_KEY = "approved";
    
    
    public DBObject toDBObject(SourceRequest sourceRequest) {
        DBObject dbo = new BasicDBObject();
        TranslatorUtils.from(dbo, MongoConstants.ID, sourceRequest.getId().longValue());
        TranslatorUtils.from(dbo, APPID_KEY, sourceRequest.getAppId().longValue());
        TranslatorUtils.from(dbo, SOURCE_KEY, sourceRequest.getSource().key());
        TranslatorUtils.from(dbo, USAGE_TYPE_KEY, sourceRequest.getUsageType().toString());
        TranslatorUtils.from(dbo, EMAIL_KEY, sourceRequest.getEmail());
        TranslatorUtils.from(dbo, APPURL_KEY, sourceRequest.getAppUrl());
        TranslatorUtils.from(dbo, REASON_KEY, sourceRequest.getReason());
        TranslatorUtils.from(dbo, APPROVED_KEY, sourceRequest.isApproved());
        return dbo;
    }
    
    public SourceRequest fromDBObject(DBObject dbo) {
        if (dbo == null) {
            return null;
        }
        return SourceRequest.builder()
                .withId(Id.valueOf(TranslatorUtils.toLong(dbo, MongoConstants.ID)))                
                .withAppId(Id.valueOf(TranslatorUtils.toLong(dbo, APPID_KEY)))
                .withSource(Publisher.fromPossibleKey(TranslatorUtils.toString(dbo, SOURCE_KEY)).get())
                .withUsageType(UsageType.valueOf(TranslatorUtils.toString(dbo, USAGE_TYPE_KEY)))
                .withEmail(TranslatorUtils.toString(dbo, EMAIL_KEY))
                .withAppUrl(TranslatorUtils.toString(dbo, APPURL_KEY))
                .withReason(TranslatorUtils.toString(dbo, REASON_KEY))
                .withApproved(TranslatorUtils.toBoolean(dbo, APPROVED_KEY))
                .build();
    }
}
