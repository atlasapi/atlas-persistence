package org.atlasapi.persistence.media.entity;

import org.atlasapi.media.entity.ContentGroup;
import org.atlasapi.media.entity.EntityType;
import org.atlasapi.persistence.ModelTranslator;

import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.DBObject;

public class ContentGroupTranslator implements ModelTranslator<ContentGroup> {

	public static final String CONTENT_URIS_KEY = "contentUris";
	
	private final DescribedTranslator contentTranslator;

	public ContentGroupTranslator() {
		this(new DescribedTranslator(new DescriptionTranslator()));
	}
	
	public ContentGroupTranslator(DescribedTranslator contentTranslator) {
		this.contentTranslator = contentTranslator;
	}
	
    @Override
    public ContentGroup fromDBObject(DBObject dbObject, ContentGroup entity) {
        contentTranslator.fromDBObject(dbObject, entity);
        entity.setContentUris(TranslatorUtils.toList(dbObject, CONTENT_URIS_KEY));
        return entity;
    }

    @Override
    public DBObject toDBObject(DBObject dbObject, ContentGroup entity) {
    	dbObject = contentTranslator.toDBObject(dbObject, entity);
    	TranslatorUtils.fromList(dbObject, entity.getContentUris(), CONTENT_URIS_KEY);
    	dbObject.put(DescribedTranslator.TYPE_KEY, EntityType.from(entity));
    	return dbObject;
    }
}
