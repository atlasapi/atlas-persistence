package org.atlasapi.persistence.media.entity;

import static com.metabroadcast.common.persistence.mongo.MongoConstants.ID;

import static org.atlasapi.persistence.media.entity.IdentifiedTranslator.OPAQUE_ID;
import static org.atlasapi.persistence.media.entity.IdentifiedTranslator.PUBLISHER; 
import static org.atlasapi.persistence.media.entity.IdentifiedTranslator.TYPE;

import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.ApiContentFields;
import org.atlasapi.persistence.ModelTranslator;
import org.atlasapi.persistence.content.ContentCategory;

import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;


public class LookupRefTranslator implements ModelTranslator<LookupRef> {

    @Override
    public DBObject toDBObject(DBObject dbObject, LookupRef model) {
        if (dbObject == null) {
            dbObject = new BasicDBObject();
        }
        
        TranslatorUtils.from(dbObject, ID, model.uri());
        TranslatorUtils.from(dbObject, PUBLISHER, model.publisher().key());
        TranslatorUtils.from(dbObject, TYPE, model.category().toString());
        TranslatorUtils.from(dbObject, OPAQUE_ID, model.id());
        
        return dbObject;
    }

    @Override
    public LookupRef fromDBObject(DBObject dbObject, LookupRef model) {
        if (model != null) {
            // I hate the ModelTranslator interface
            throw new IllegalArgumentException("Cannot mutate an existing LookupRef");
        }
        
        String uri = TranslatorUtils.toString(dbObject, ID);
        Long id = TranslatorUtils.toLong(dbObject, OPAQUE_ID);
        Publisher publisher = Publisher.fromKey(TranslatorUtils.toString(dbObject, PUBLISHER))
                .requireValue();
        String type = TranslatorUtils.toString(dbObject, TYPE);
        return new LookupRef(uri, id, publisher, ContentCategory.valueOf(type));
    }

    @Override
    public DBObject unsetFields(DBObject dbObject, Iterable<ApiContentFields> fieldsToUnset) {
        return dbObject;
    }

}
