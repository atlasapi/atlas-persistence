package org.atlasapi.persistence.media.entity;

import org.atlasapi.persistence.tracking.ContentMention;
import org.atlasapi.persistence.tracking.TrackingSource;

import com.metabroadcast.common.persistence.translator.ModelTranslator;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class ContentMentionTranslator implements ModelTranslator<ContentMention> {

    @Override
    public ContentMention fromDBObject(DBObject dbObject, ContentMention entity) {
        return new ContentMention(
                (String) dbObject.get("uri"), TrackingSource.valueOf((String) dbObject.get("source")), 
                (String) dbObject.get("externalRef"), TranslatorUtils.toDateTime(dbObject, "when"));
    }

    @Override
    public DBObject toDBObject(DBObject dbObject, ContentMention entity) {
        if (dbObject == null) {
            dbObject = new BasicDBObject();
        }
        
        TranslatorUtils.from(dbObject, "uri", entity.uri());
        TranslatorUtils.from(dbObject, "source", entity.source().name());
        TranslatorUtils.from(dbObject, "externalRef", entity.externalRef());
        TranslatorUtils.fromDateTime(dbObject, "when", entity.mentionedAt());
        
        return dbObject;
    }
}
