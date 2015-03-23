package org.atlasapi.persistence.media.entity;

import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.DBObject;
import org.atlasapi.media.entity.TermsOfUse;
import org.atlasapi.persistence.ModelTranslator;

public class TermsOfUseTranslator implements ModelTranslator<TermsOfUse> {
    private static final String TEXT = "text";

    @Override
    public DBObject toDBObject(DBObject dbObject, TermsOfUse model) {
        TranslatorUtils.from(dbObject, TEXT, model.getText());
        return dbObject;
    }

    @Override
    public TermsOfUse fromDBObject(DBObject dbObject, TermsOfUse model) {
        return new TermsOfUse(TranslatorUtils.toString(dbObject, TEXT));
    }
}
