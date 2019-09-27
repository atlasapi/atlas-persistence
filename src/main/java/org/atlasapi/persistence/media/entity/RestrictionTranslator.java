package org.atlasapi.persistence.media.entity;

import org.atlasapi.media.entity.Restriction;
import org.atlasapi.persistence.ApiContentFields;
import org.atlasapi.persistence.ModelTranslator;

import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.DBObject;

public class RestrictionTranslator implements ModelTranslator<Restriction> {
	
    private static final String MESSAGE = "message";
	private static final String MINIMUM_AGE = "minimumAge";
	private static final String RESTRICTED = "restricted";
	private static final String AUTHORITY = "authority";
	private static final String RATING = "rating";
	
	private final IdentifiedTranslator descriptionTranslator = new IdentifiedTranslator();

	@Override
	public DBObject toDBObject(DBObject dbObject, Restriction model) {
		dbObject = descriptionTranslator.toDBObject(dbObject, model);
		
		TranslatorUtils.from(dbObject, RESTRICTED, model.isRestricted());
		TranslatorUtils.from(dbObject, MINIMUM_AGE, model.getMinimumAge());
		TranslatorUtils.from(dbObject, MESSAGE, model.getMessage());
		TranslatorUtils.from(dbObject, AUTHORITY, model.getAuthority());
		TranslatorUtils.from(dbObject, RATING, model.getRating());
		
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
		model.setAuthority(TranslatorUtils.toString(dbObject, AUTHORITY));
		model.setRating(TranslatorUtils.toString(dbObject, RATING));
		
		return model;
	}

	@Override
	public DBObject unsetFields(DBObject dbObject, Iterable<ApiContentFields> fieldsToUnset) {
		return dbObject;
	}

}
