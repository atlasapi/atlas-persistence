package org.atlasapi.media.segment;

import java.math.BigInteger;

import org.atlasapi.persistence.ApiContentFields;
import org.atlasapi.persistence.ModelTranslator;
import org.atlasapi.persistence.media.entity.IdentifiedTranslator;
import org.joda.time.Duration;

import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class SegmentEventTranslator implements ModelTranslator<SegmentEvent> {

    private static final String SEGMENT_KEY = "segment";
    private static final String DESCRIPTION_KEY = "description";
    private static final String POSITION_KEY = "position";
    private static final String OFFSET_KEY = "offset";
    private static final String CHAPTER_KEY = "chapter";

    private final IdentifiedTranslator identifiedTranslator = new IdentifiedTranslator();
    private final DescriptionTranslator descriptionTranslator = new DescriptionTranslator();
    private final NumberToShortStringCodec idCodec;

    public SegmentEventTranslator(NumberToShortStringCodec idCodec) {
        this.idCodec = idCodec;
    }

    @Override
    public DBObject toDBObject(DBObject dbo, SegmentEvent model) {
        if (dbo == null) {
            dbo = new BasicDBObject();
        }

        identifiedTranslator.toDBObject(dbo, model);

        TranslatorUtils.from(dbo, SEGMENT_KEY, model.getSegment().identifier());
        TranslatorUtils.from(dbo, DESCRIPTION_KEY, descriptionTranslator.toDBObject(model.getDescription()));
        TranslatorUtils.from(dbo, POSITION_KEY, model.getPosition());
        if (model.getOffset() != null) {
            TranslatorUtils.from(dbo, OFFSET_KEY, model.getOffset().getMillis());
        }
        TranslatorUtils.from(dbo, CHAPTER_KEY, model.getIsChapter());

        return dbo;
    }

    @Override
    public SegmentEvent fromDBObject(DBObject dbo, SegmentEvent model) {
        if (model == null) {
            model = new SegmentEvent();
        }

        identifiedTranslator.fromDBObject(dbo, model);

        model.setSegment(new SegmentRef(TranslatorUtils.toLong(dbo, SEGMENT_KEY)));
        model.setDescription(descriptionTranslator.fromDBObject(TranslatorUtils.toDBObject(dbo, DESCRIPTION_KEY)));
        model.setPosition(TranslatorUtils.toInteger(dbo, POSITION_KEY));
        model.setIsChapter(TranslatorUtils.toBoolean(dbo, CHAPTER_KEY));
        
        Long rawOffset = TranslatorUtils.toLong(dbo, OFFSET_KEY);
        if (rawOffset != null) {
            model.setOffset(new Duration(rawOffset));
        }

        return model;
    }

    @Override
    public DBObject unsetFields(DBObject dbObject, Iterable<ApiContentFields> fieldsToUnset) {
        return dbObject;
    }

}
