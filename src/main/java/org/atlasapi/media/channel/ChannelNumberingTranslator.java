package org.atlasapi.media.channel;

import org.atlasapi.media.common.Id;

import com.metabroadcast.common.persistence.mongo.MongoConstants;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class ChannelNumberingTranslator {
    private static final String CHANNEL_NUMBER_KEY = "channelNumber";
    private static final String CHANNEL_KEY = "channel";
    private static final String CHANNEL_GROUP_KEY = "channelGroup";
    private static final String START_DATE_KEY = "startDate";
    private static final String END_DATE_KEY = "endDate";

    public DBObject toDBObject(ChannelNumbering model) {
        DBObject dbo = new BasicDBObject();
        
        TranslatorUtils.from(dbo, CHANNEL_NUMBER_KEY, model.getChannelNumber());
        TranslatorUtils.from(dbo, CHANNEL_KEY, new BasicDBObject(MongoConstants.ID, model.getChannel().longValue()));
        TranslatorUtils.from(dbo, CHANNEL_GROUP_KEY, new BasicDBObject(MongoConstants.ID, model.getChannelGroup().longValue()));
        TranslatorUtils.fromLocalDate(dbo, START_DATE_KEY, model.getStartDate());
        TranslatorUtils.fromLocalDate(dbo, END_DATE_KEY, model.getEndDate());
        
        return dbo;
    }

    public ChannelNumbering fromDBObject(DBObject dbo) {
        return ChannelNumbering.builder()
            .withChannelNumber(TranslatorUtils.toString(dbo, CHANNEL_NUMBER_KEY))
            .withChannel(Id.valueOf(TranslatorUtils.toLong(TranslatorUtils.toDBObject(dbo, CHANNEL_KEY), MongoConstants.ID)))
            .withChannelGroup(Id.valueOf(TranslatorUtils.toLong(TranslatorUtils.toDBObject(dbo, CHANNEL_GROUP_KEY), MongoConstants.ID)))
            .withStartDate(TranslatorUtils.toLocalDate(dbo, START_DATE_KEY))
            .withEndDate(TranslatorUtils.toLocalDate(dbo, END_DATE_KEY))
            .build();
    }

}
