package org.atlasapi.persistence.media.entity;

import org.atlasapi.media.entity.Distribution;
import org.atlasapi.persistence.ModelTranslator;

import com.metabroadcast.common.persistence.translator.TranslatorUtils;

import com.mongodb.DBObject;
import org.joda.time.DateTime;

public class DistributionTranslator {

    protected static final String FORMAT = "format";
    protected static final String RELEASE_DATE = "releaseDate";
    protected static final String DISTRIBUTOR = "distributor";

    public DBObject toDBObject(DBObject dbObject, Distribution model) {
        TranslatorUtils.from(dbObject, FORMAT, model.getFormat());
        TranslatorUtils.from(dbObject, RELEASE_DATE, model.getReleaseDate());
        TranslatorUtils.from(dbObject, DISTRIBUTOR, model.getDistributor());

        return dbObject;
    }

    public Distribution fromDBObject(DBObject dbObject) {
        return Distribution.builder()
                .withFormat(TranslatorUtils.toString(dbObject, FORMAT))
                .withReleaseDate((DateTime) dbObject.get(RELEASE_DATE))
                .withDistributor(TranslatorUtils.toString(dbObject, DISTRIBUTOR))
                .build();
    }

}
