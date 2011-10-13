package org.atlasapi.persistence.media.entity;

import org.atlasapi.media.entity.Topic;

import com.metabroadcast.common.persistence.mongo.MongoConstants;
import com.metabroadcast.common.persistence.translator.ModelTranslator;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class TopicTranslator implements ModelTranslator<Topic> {

    public static final String VALUE = "value";
    public static final String NAMESPACE = "ns";
    public static final String TOPIC_TYPE = "topicType";
    
    private DescribedTranslator describedTranslator;

    public TopicTranslator() {
        this.describedTranslator = new DescribedTranslator(new DescriptionTranslator());
    }
    
    public DBObject toDBObject(Topic model) {
        return this.toDBObject(new BasicDBObject(), model);
    }
    
    @Override
    public DBObject toDBObject(DBObject dbObject, Topic model) {
        if (dbObject == null) {
            dbObject = new BasicDBObject();
        }
        
        describedTranslator.toDBObject(dbObject, model);
        
        TranslatorUtils.from(dbObject, TOPIC_TYPE, model.getType().key());
        TranslatorUtils.from(dbObject, NAMESPACE, model.getNamespace());
        TranslatorUtils.from(dbObject, VALUE, model.getValue());
        
        return dbObject;
    }
    
    public Topic fromDBObject(DBObject dbObject) {
        return this.fromDBObject(dbObject, null);
    }

    @Override
    public Topic fromDBObject(DBObject dbObject, Topic model) {
        if (model == null) {
            model = new Topic(TranslatorUtils.toString(dbObject, MongoConstants.ID));
        }
        
        describedTranslator.fromDBObject(dbObject, model);
        
        model.setType(Topic.Type.fromKey(TranslatorUtils.toString(dbObject, TOPIC_TYPE)));
        model.setNamespace(TranslatorUtils.toString(dbObject, NAMESPACE));
        model.setValue(TranslatorUtils.toString(dbObject, VALUE));
        
        return model;
    }

}
