package org.atlasapi.persistence.media.entity;

import static org.atlasapi.persistence.media.entity.IdentifiedTranslator.CANONICAL_URL;

import org.atlasapi.media.entity.Image;
import org.atlasapi.media.entity.ImageAspectRatio;
import org.atlasapi.media.entity.ImageBackground;
import org.atlasapi.media.entity.ImageColor;
import org.atlasapi.media.entity.ImageType;
import org.atlasapi.persistence.ModelTranslator;

import com.metabroadcast.common.media.MimeType;
import com.metabroadcast.common.persistence.mongo.MongoConstants;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class ImageTranslator implements ModelTranslator<Image> {

    private static final String AVAILABILITY_START = "availabilityStart";
    private static final String AVAILABILITY_END = "availabilityEnd";
    private static final String HEIGHT = "height";
    private static final String WIDTH = "width";
    private static final String IMAGE_TYPE = "type";
    private static final String COLOR = "color";
    private static final String BACKGROUND = "background";
    private static final String ASPECT_RATIO = "aspectRatio";
    private static final String MIME_TYPE = "mimeType";

    private IdentifiedTranslator identifiedTranslator = new IdentifiedTranslator(false);

    @Override
    public DBObject toDBObject(DBObject dbObject, Image model) {
        if (dbObject == null) {
            dbObject = new BasicDBObject();
        }
        
        identifiedTranslator.toDBObject(dbObject, model);
        TranslatorUtils.from(dbObject, CANONICAL_URL, model.getCanonicalUri());

        TranslatorUtils.fromDateTime(dbObject, AVAILABILITY_START, model.getAvailabilityStart());
        TranslatorUtils.fromDateTime(dbObject, AVAILABILITY_END, model.getAvailabilityEnd());
        TranslatorUtils.from(dbObject, HEIGHT, model.getHeight());
        TranslatorUtils.from(dbObject, WIDTH, model.getWidth());

        String type = model.getType() != null ? model.getType().toString().toLowerCase()
                                             : null;
        TranslatorUtils.from(dbObject, IMAGE_TYPE, type);

        String color = model.getColor() != null ? model.getColor().toString().toLowerCase() : null;
        TranslatorUtils.from(dbObject, COLOR, color);

        String bg = model.getBackground() != null ? model.getBackground()
                .toString()
                .toLowerCase() : null;

        TranslatorUtils.from(dbObject, BACKGROUND, bg);

        String aspectRatio = model.getAspectRatio() != null ? model.getAspectRatio()
                .toString()
                .toLowerCase() : null;
        TranslatorUtils.from(dbObject, ASPECT_RATIO, aspectRatio);

        String mimeType = model.getMimeType() != null ? model.getMimeType()
                .toString()
                .toLowerCase() : null;
        TranslatorUtils.from(dbObject, MIME_TYPE, mimeType);

        return dbObject;
    }

    @Override
    public Image fromDBObject(DBObject dbObject, Image model) {

        if(model == null) {
            model = new Image(TranslatorUtils.toString(MongoConstants.ID));
        }
        
        identifiedTranslator.fromDBObject(dbObject, model);
        model.setCanonicalUri((String) dbObject.get(CANONICAL_URL));

        model.setAvailabilityStart(TranslatorUtils.toDateTime(dbObject, AVAILABILITY_START));
        model.setAvailabilityEnd(TranslatorUtils.toDateTime(dbObject, AVAILABILITY_END));
        model.setHeight(TranslatorUtils.toInteger(dbObject, HEIGHT));
        model.setWidth(TranslatorUtils.toInteger(dbObject, WIDTH));

        String imageType = TranslatorUtils.toString(dbObject, IMAGE_TYPE);
        if (imageType != null) {
            model.setType(ImageType.valueOf(imageType.toUpperCase()));
        }

        String color = TranslatorUtils.toString(dbObject, COLOR);
        if (color != null) {
            model.setColor(ImageColor.valueOf(color.toUpperCase()));
        }

        String background = TranslatorUtils.toString(dbObject, BACKGROUND);
        if (background != null) {
            model.setBackground(ImageBackground.valueOf(background.toUpperCase()));
        }

        String aspectRatio = TranslatorUtils.toString(dbObject, ASPECT_RATIO);
        if (aspectRatio != null) {
            model.setAspectRatio(ImageAspectRatio.valueOf(aspectRatio.toUpperCase()));
        }

        String mimeType = TranslatorUtils.toString(dbObject, MIME_TYPE);
        if (mimeType != null) {
            model.setMimeType(MimeType.fromString(mimeType.toUpperCase()));
        }

        return model;
    }

}
