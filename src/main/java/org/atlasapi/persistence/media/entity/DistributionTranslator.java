package org.atlasapi.persistence.media.entity;

import org.atlasapi.media.entity.Distribution;
import org.atlasapi.persistence.ModelTranslator;

import com.metabroadcast.common.persistence.translator.TranslatorUtils;

import com.mongodb.DBObject;

public class DistributionTranslator implements ModelTranslator<Distribution> {

    protected static final String FORMAT = "format";
    protected static final String RELEASE_DATE = "releaseDate";
    protected static final String DISTRIBUTOR = "distributor";

    @Override
    public DBObject toDBObject(DBObject dbObject, Distribution model) {
        TranslatorUtils.from(dbObject, FORMAT, model.getFormat());
        TranslatorUtils.fromDateTime(dbObject, RELEASE_DATE, model.getReleaseDate());
        TranslatorUtils.from(dbObject, DISTRIBUTOR, model.getDistributor());

        return dbObject;
    }

    @Override
    public Distribution fromDBObject(DBObject dbObject, Distribution model) {
        model.setFormat(TranslatorUtils.toString(dbObject, FORMAT));
        model.setReleaseDate(TranslatorUtils.toDateTime(dbObject, RELEASE_DATE));
        model.setDistributor(TranslatorUtils.toString(dbObject, DISTRIBUTOR));

        return model;
    }

}
