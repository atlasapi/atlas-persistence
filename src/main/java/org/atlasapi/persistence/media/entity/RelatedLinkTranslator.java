package org.atlasapi.persistence.media.entity;

import static org.atlasapi.media.content.RelatedLink.relatedLink;

import org.atlasapi.media.content.RelatedLink;
import org.atlasapi.media.content.RelatedLink.LinkType;

import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;


public class RelatedLinkTranslator {

    private static final String THUMB_KEY = "thumb";
    private static final String IMAGE_KEY = "image";
    private static final String DESC_KEY = "desc";
    private static final String TITLE_KEY = "title";
    private static final String SHORT_NAME_KEY = "shortName";
    private static final String SOURCE_ID_KEY = "sourceId";
    private static final String TYPE_KEY = "type";
    private static final String URL_KEY = "url";

    public DBObject toDBObject(RelatedLink link) {
        
        DBObject dbo = new BasicDBObject();
        
        TranslatorUtils.from(dbo, URL_KEY, link.getUrl());
        TranslatorUtils.from(dbo, TYPE_KEY, link.getType().toString().toLowerCase());
        
        TranslatorUtils.from(dbo, SOURCE_ID_KEY, link.getSourceId());
        TranslatorUtils.from(dbo, SHORT_NAME_KEY, link.getShortName());
        
        TranslatorUtils.from(dbo, TITLE_KEY, link.getTitle());
        TranslatorUtils.from(dbo, DESC_KEY, link.getDescription());
        TranslatorUtils.from(dbo, IMAGE_KEY, link.getImage());
        TranslatorUtils.from(dbo, THUMB_KEY, link.getThumbnail());
        
        return dbo;
    }
    
    public RelatedLink fromDBObject(DBObject dbo) {
        return relatedLink(LinkType.valueOf(TranslatorUtils.toString(dbo, TYPE_KEY).toUpperCase()), TranslatorUtils.toString(dbo, URL_KEY))
               .withSourceId(TranslatorUtils.toString(dbo, SOURCE_ID_KEY))
               .withShortName(TranslatorUtils.toString(dbo, SHORT_NAME_KEY))
               .withTitle(TranslatorUtils.toString(dbo, TITLE_KEY))
               .withDescription(TranslatorUtils.toString(dbo, DESC_KEY))
               .withImage(TranslatorUtils.toString(dbo, IMAGE_KEY))
               .withThumbnail(TranslatorUtils.toString(dbo, THUMB_KEY))
           .build();
    }

}
