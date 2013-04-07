package org.atlasapi.persistence.media.entity;

import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import org.atlasapi.media.common.Id;
import org.atlasapi.media.entity.ContentGroupRef;

public class ContentGroupRefTranslator {

    private static final String ID_KEY = "id";
    private static final String URI_KEY = "uri";

    public DBObject toDBObject(ContentGroupRef ref) {
        DBObject dbo = new BasicDBObject();
        TranslatorUtils.from(dbo, ID_KEY, ref.getId().longValue());
        TranslatorUtils.from(dbo, URI_KEY, ref.getUri());

        return dbo;
    }

    public ContentGroupRef fromDBObject(DBObject dbo) {
        return new ContentGroupRef(
            Id.valueOf(TranslatorUtils.toLong(dbo, ID_KEY)), 
            TranslatorUtils.toString(dbo, URI_KEY)
        );
    }
}