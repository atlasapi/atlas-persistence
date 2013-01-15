package org.atlasapi.persistence.media.channel;

import org.atlasapi.media.channel.ChannelNumbering;
import org.atlasapi.persistence.media.ModelTranslator;

import com.metabroadcast.common.persistence.mongo.MongoConstants;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class ChannelNumberingTranslator implements ModelTranslator<ChannelNumbering> {
    private static final String CHANNEL_NUMBER_KEY = "channelNumber";
    private static final String CHANNEL_KEY = "channel";
    private static final String CHANNEL_GROUP_KEY = "channelGroup";
    private static final String START_DATE_KEY = "startDate";
    private static final String END_DATE_KEY = "endDate";

    @Override
    public DBObject toDBObject(DBObject dbObject, ChannelNumbering model) {
        if (dbObject == null) {
            dbObject = new BasicDBObject();
        }
        
        TranslatorUtils.from(dbObject, CHANNEL_NUMBER_KEY, model.getChannelNumber());
        TranslatorUtils.from(dbObject, CHANNEL_KEY, new BasicDBObject(MongoConstants.ID, model.getChannel()));
        TranslatorUtils.from(dbObject, CHANNEL_GROUP_KEY, new BasicDBObject(MongoConstants.ID, model.getChannelGroup()));
        TranslatorUtils.fromLocalDate(dbObject, START_DATE_KEY, model.getStartDate());
        TranslatorUtils.fromLocalDate(dbObject, END_DATE_KEY, model.getEndDate());
        
        return dbObject;
    }

    @Override
    public ChannelNumbering fromDBObject(DBObject dbObject, ChannelNumbering model) {
        if (dbObject == null) {
            return null;
        }
        
        if (model == null) {
            model = ChannelNumbering.builder()
                .withChannelNumber(TranslatorUtils.toInteger(dbObject, CHANNEL_NUMBER_KEY))
                .withChannel(TranslatorUtils.toLong(TranslatorUtils.toDBObject(dbObject, CHANNEL_KEY), MongoConstants.ID))
                .withChannelGroup(TranslatorUtils.toLong(TranslatorUtils.toDBObject(dbObject, CHANNEL_GROUP_KEY), MongoConstants.ID))
                .withStartDate(TranslatorUtils.toLocalDate(dbObject, START_DATE_KEY))
                .withEndDate(TranslatorUtils.toLocalDate(dbObject, END_DATE_KEY))
                .build();
        } else {
            model.setChannelNumber(TranslatorUtils.toInteger(dbObject, CHANNEL_NUMBER_KEY));
            model.setChannel(TranslatorUtils.toLong(TranslatorUtils.toDBObject(dbObject, CHANNEL_KEY), MongoConstants.ID));
            model.setChannelGroup(TranslatorUtils.toLong(TranslatorUtils.toDBObject(dbObject, CHANNEL_GROUP_KEY), MongoConstants.ID));
            model.setStartDate(TranslatorUtils.toLocalDate(dbObject, START_DATE_KEY));
            model.setEndDate(TranslatorUtils.toLocalDate(dbObject, END_DATE_KEY));
        }
        
        
        
        return model;
    }

}
