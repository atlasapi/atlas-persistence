package org.atlasapi.persistence.media.entity;

import org.atlasapi.media.entity.ContentGroup;
import org.atlasapi.media.entity.EntityType;
import org.atlasapi.persistence.ModelTranslator;

import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.DBObject;

public class ContentGroupTranslator implements ModelTranslator<ContentGroup> {

    public static final String GROUP_TYPE_KEY = "groupType";
    public static final String CONTENT_URIS_KEY = "childRefs";
    private final DescribedTranslator contentTranslator;
    private final ChildRefTranslator childTranslator;

    public ContentGroupTranslator() {
        this(true);
    }
    
    protected ContentGroupTranslator(boolean useAtlasIdAsId) {
        this.contentTranslator = new DescribedTranslator(new IdentifiedTranslator(useAtlasIdAsId), new ImageTranslator());
        this.childTranslator = new ChildRefTranslator();
    }

    @Override
    public ContentGroup fromDBObject(DBObject dbObject, ContentGroup entity) {
        if (entity == null) {
            entity = new ContentGroup();
        }
        contentTranslator.fromDBObject(dbObject, entity);
        entity.setType(ContentGroup.Type.valueOf(TranslatorUtils.toString(dbObject, GROUP_TYPE_KEY)));
        entity.setContents(childTranslator.fromDBObjects(TranslatorUtils.toDBObjectList(dbObject, CONTENT_URIS_KEY)));
        entity.setReadHash(generateHashByRemovingFieldsFromTheDbo(dbObject));
        return entity;
    }

    @Override
    public DBObject toDBObject(DBObject dbObject, ContentGroup entity) {
        dbObject = contentTranslator.toDBObject(dbObject, entity);
        dbObject.put(DescribedTranslator.TYPE_KEY, EntityType.from(entity).toString());
        TranslatorUtils.from(dbObject, GROUP_TYPE_KEY, entity.getType().toString());
        TranslatorUtils.from(dbObject, CONTENT_URIS_KEY, childTranslator.toDBList(entity.getContents()));
        return dbObject;
    }

    public String hashCodeOf(ContentGroup contentGroup) {
        return generateHashByRemovingFieldsFromTheDbo(toDBObject(null, contentGroup));
    }

    private String generateHashByRemovingFieldsFromTheDbo(DBObject dbObject) {
        dbObject.removeField(DescribedTranslator.LAST_FETCHED_KEY);
        dbObject.removeField(DescribedTranslator.THIS_OR_CHILD_LAST_UPDATED_KEY);
        dbObject.removeField(IdentifiedTranslator.LAST_UPDATED);
        return String.valueOf(dbObject.hashCode());
    }
}
