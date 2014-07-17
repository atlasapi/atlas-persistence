package org.atlasapi.media.channel;

import java.util.List;
import java.util.Set;

import com.google.common.collect.Sets;
import com.metabroadcast.common.persistence.mongo.MongoConstants;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class ChannelNumberingTranslator {
    private static final String CHANNEL_NUMBER_KEY = "channelNumber";
    public static final String CHANNEL_KEY = "channel";
    public static final String CHANNEL_GROUP_KEY = "channelGroup";
    private static final String START_DATE_KEY = "startDate";
    private static final String END_DATE_KEY = "endDate";

    public DBObject toDBObject(ChannelNumbering model) {
        DBObject dbo = new BasicDBObject();
        
        TranslatorUtils.from(dbo, CHANNEL_NUMBER_KEY, model.getChannelNumber());
        TranslatorUtils.from(dbo, CHANNEL_KEY, new BasicDBObject(MongoConstants.ID, model.getChannel()));
        TranslatorUtils.from(dbo, CHANNEL_GROUP_KEY, new BasicDBObject(MongoConstants.ID, model.getChannelGroup()));
        TranslatorUtils.fromLocalDate(dbo, START_DATE_KEY, model.getStartDate());
        TranslatorUtils.fromLocalDate(dbo, END_DATE_KEY, model.getEndDate());
        
        return dbo;
    }

    public ChannelNumbering fromDBObject(DBObject dbo) {
        return ChannelNumbering.builder()
            .withChannelNumber(TranslatorUtils.toString(dbo, CHANNEL_NUMBER_KEY))
            .withChannel(TranslatorUtils.toLong(TranslatorUtils.toDBObject(dbo, CHANNEL_KEY), MongoConstants.ID))
            .withChannelGroup(TranslatorUtils.toLong(TranslatorUtils.toDBObject(dbo, CHANNEL_GROUP_KEY), MongoConstants.ID))
            .withStartDate(TranslatorUtils.toLocalDate(dbo, START_DATE_KEY))
            .withEndDate(TranslatorUtils.toLocalDate(dbo, END_DATE_KEY))
            .build();
    }
    
    public void fromChannelNumberingSet(DBObject dbObject, String key, Set<ChannelNumbering> set) {
        BasicDBList values = new BasicDBList();
        for (ChannelNumbering value : set) {
            if (value != null) {
                values.add(toDBObject(value));
            }
        }
        dbObject.put(key, values);
    }
    
    @SuppressWarnings("unchecked")
    public Set<ChannelNumbering> toChannelNumberingSet(DBObject object, String name) {
        Set<ChannelNumbering> channelNumbers = Sets.newLinkedHashSet();
        if (object.containsField(name)) {
            for (DBObject element : (List<DBObject>) object.get(name)) {
                channelNumbers.add(fromDBObject(element));
            }
            return channelNumbers;
        }
        return Sets.newLinkedHashSet();
    }

}
