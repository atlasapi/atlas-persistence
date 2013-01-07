package org.atlasapi.media.channel;

import org.atlasapi.persistence.ModelTranslator;

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
        TranslatorUtils.from(dbObject, CHANNEL_KEY, model.getChannel());
        TranslatorUtils.from(dbObject, CHANNEL_GROUP_KEY, model.getChannelGroup());
        TranslatorUtils.from(dbObject, START_DATE_KEY, model.getStartDate());
        TranslatorUtils.from(dbObject, END_DATE_KEY, model.getEndDate());
        
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
                .withChannel(TranslatorUtils.toLong(dbObject, CHANNEL_KEY))
                .withChannelGroup(TranslatorUtils.toLong(dbObject, CHANNEL_GROUP_KEY))
                .withStartDate(TranslatorUtils.toDateTime(dbObject, START_DATE_KEY))
                .withEndDate(TranslatorUtils.toDateTime(dbObject, END_DATE_KEY))
                .build();
        } else {
            model.setChannelNumber(TranslatorUtils.toInteger(dbObject, CHANNEL_NUMBER_KEY));
            model.setChannel(TranslatorUtils.toLong(dbObject, CHANNEL_KEY));
            model.setChannelGroup(TranslatorUtils.toLong(dbObject, CHANNEL_GROUP_KEY));
            model.setStartDate(TranslatorUtils.toDateTime(dbObject, START_DATE_KEY));
            model.setEndDate(TranslatorUtils.toDateTime(dbObject, END_DATE_KEY));
        }
        
        
        
        return model;
    }

}
