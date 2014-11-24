package org.atlasapi.media.segment;

import java.math.BigInteger;

import org.atlasapi.media.SegmentType;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.media.entity.IdentifiedTranslator;
import org.joda.time.Duration;

import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.persistence.translator.ModelTranslator;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class SegmentTranslator implements ModelTranslator<Segment> {

    public static final String SOURCE_ID_KEY = "sourceId";
    public static final String PUBLISHER_KEY = "publisher";
    private static final String DESCRIPTION_KEY = "description";
    private static final String TYPE_KEY = "type";
    private static final String DURATION_KEY = "duration";
    
    private final IdentifiedTranslator identifiedTranslator = new IdentifiedTranslator(true);
    private final NumberToShortStringCodec idCodec;

    public SegmentTranslator(NumberToShortStringCodec idCodec) {
        this.idCodec = idCodec;
    }
    
    @Override
    public DBObject toDBObject(DBObject dbo, Segment model) {
        if (dbo == null) {
            dbo = new BasicDBObject();
        }
        
        identifiedTranslator.toDBObject(dbo, model);

        if (model.getPublisher() != null) {
            TranslatorUtils.from(dbo, PUBLISHER_KEY, model.getPublisher().key());
        }

        if (model.getDescription() != null) {
            TranslatorUtils.from(dbo, DESCRIPTION_KEY, model.getDescription());
        }

        if (model.getType() != null) {
            TranslatorUtils.from(dbo, TYPE_KEY, model.getType().toString());
        }
        
        if (model.getDuration() != null) {
            TranslatorUtils.from(dbo, DURATION_KEY, model.getDuration().getMillis());
        }
        
        return dbo;
    }

    @Override
    public Segment fromDBObject(DBObject dbo, Segment model) {
        if (model == null) {
            model = new Segment();
        }
        
        identifiedTranslator.fromDBObject(dbo, model);

        final Long rawDuration = TranslatorUtils.toLong(dbo, DURATION_KEY);
        if (rawDuration != null) {
            model.setDuration(new Duration(rawDuration));
        }
        model.setPublisher(Publisher.fromKey(TranslatorUtils.toString(dbo, PUBLISHER_KEY)).valueOrNull());
        model.setType(SegmentType.fromString(TranslatorUtils.toString(dbo, TYPE_KEY)).valueOrNull());
        model.setDescription(TranslatorUtils.toString(dbo, DESCRIPTION_KEY));
        
        return model;
    }
    
}
