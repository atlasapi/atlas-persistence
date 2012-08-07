package org.atlasapi.persistence.media.segment;

import static org.atlasapi.media.entity.Description.description;

import org.atlasapi.media.entity.Description;

import com.google.common.base.Strings;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class DescriptionTranslator {

    private static final String THUMBNAIL_KEY = "thumb";
    private static final String IMAGE_KEY = "image";
    private static final String LONG_SYNOPSIS_KEY = "long";
    private static final String MEDIUM_SYNOPSIS_KEY = "medium";
    private static final String SHORT_SYNOPSIS_KEY = "short";
    private static final String TITLE_KEY = "title";

    public DBObject toDBObject(Description desc) {
        if (desc == null || Description.EMPTY.equals(desc)) {
            return null;
        }
        
        BasicDBObject dbo = new BasicDBObject();
        
        addEmptyToNullField(dbo, TITLE_KEY, desc.getTitle());
        addEmptyToNullField(dbo, SHORT_SYNOPSIS_KEY, desc.getShortSynopsis());
        addEmptyToNullField(dbo, MEDIUM_SYNOPSIS_KEY, desc.getMediumSynopsis());
        addEmptyToNullField(dbo, LONG_SYNOPSIS_KEY, desc.getLongSynopsis());
        addEmptyToNullField(dbo, IMAGE_KEY, desc.getImage());
        addEmptyToNullField(dbo, THUMBNAIL_KEY, desc.getThumbnail());
        
        return dbo;
    }

    private void addEmptyToNullField(DBObject dbo, String field, String value) {
        TranslatorUtils.from(dbo, field, Strings.emptyToNull(value));
    }
    
    public Description fromDBObject(DBObject dbo) {
        if (dbo == null) {
            return Description.EMPTY;
        }
        
        return description()
            .withTitle(getField(dbo,TITLE_KEY))
            .withShortSynopsis(getField(dbo, SHORT_SYNOPSIS_KEY))
            .withMediumSynopsis(getField(dbo, LONG_SYNOPSIS_KEY))
            .withLongSynopsis(getField(dbo, LONG_SYNOPSIS_KEY))
            .withImage(getField(dbo, IMAGE_KEY))
            .withThumbnail(getField(dbo, THUMBNAIL_KEY)).build();
        
    }

    private String getField(DBObject dbo, String field) {
        return Strings.nullToEmpty(TranslatorUtils.toString(dbo, field));
    }
    
}
