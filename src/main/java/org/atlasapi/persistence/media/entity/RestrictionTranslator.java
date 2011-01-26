package org.atlasapi.persistence.media.entity;

import org.atlasapi.media.entity.Restriction;
import org.atlasapi.persistence.ModelTranslator;

import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.DBObject;

public class RestrictionTranslator implements ModelTranslator<Restriction> {
	
    private static final String MESSAGE = "message";
	private static final String MINIMUM_AGE = "minimumAge";
	private static final String RESTRICTED = "restricted";
	
	private final DescriptionTranslator descriptionTranslator = new DescriptionTranslator();

	@Override
	public DBObject toDBObject(DBObject dbObject, Restriction model) {
		dbObject = descriptionTranslator.toDBObject(dbObject, model);
		
		TranslatorUtils.from(dbObject, RESTRICTED, model.isRestricted());
		TranslatorUtils.from(dbObject, MINIMUM_AGE, model.getMinimumAge());
		TranslatorUtils.from(dbObject, MESSAGE, model.getMessage());
		
		return dbObject;
	}

	@Override
	public Restriction fromDBObject(DBObject dbObject, Restriction model) {
		if (model == null) {
			model = new Restriction();
		}
		
		descriptionTranslator.fromDBObject(dbObject, model);
		
		model.setRestricted(TranslatorUtils.toBoolean(dbObject, RESTRICTED));
		model.setMinimumAge(TranslatorUtils.toInteger(dbObject, MINIMUM_AGE));
		model.setMessage(TranslatorUtils.toString(dbObject, MESSAGE));
		
		return model;
	}

}
