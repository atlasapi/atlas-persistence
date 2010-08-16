package org.atlasapi.persistence.logging;

import static com.metabroadcast.common.persistence.mongo.MongoBuilders.where;
import static com.metabroadcast.common.persistence.mongo.MongoConstants.ID;
import static com.metabroadcast.common.persistence.mongo.MongoConstants.NATURAL;

import java.util.List;

import org.atlasapi.persistence.logging.AdapterLogEntry.ExceptionSummary;
import org.atlasapi.persistence.logging.AdapterLogEntry.Severity;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;


public class MongoLoggingAdapter implements AdapterLog, LogReader {

	private static final String DESCRIPTION_KEY = "description";
	private static final String TIMESTAMP_KEY = "timestamp";
	private static final String SOURCE_KEY = "source";
	private static final String URI_KEY = "uri";

	private static final String EXCEPTION_KEY = "exception";
	private static final String CAUSE_TRACE = "trace";
	private static final String CAUSE_MESSAGE = "message";
	private static final String CAUSE_CLASS_NAME = "className";
	private static final String CAUSE_PARENT = "cause";
	private static final String SEVERITY_KEY = "severity";
	
	private final DBCollection log;

	public MongoLoggingAdapter(DatabasedMongo db) {
		log = db.collection("logging");
	}
	
	@Override
	public void record(AdapterLogEntry entry) {
		log.insert(toDbObject(entry));
	}
	
	public Iterable<AdapterLogEntry> read() {
		return Iterables.transform(log.find().sort(new BasicDBObject(NATURAL, -1)), new Function<DBObject, AdapterLogEntry>() {
			@Override
			public AdapterLogEntry apply(DBObject dbo) {
				return fromDbObject(dbo);
			}
		});
	}

	private DBObject toDbObject(AdapterLogEntry entry) {
		DBObject dbo = new BasicDBObject();
		dbo.put(ID, entry.id());
		dbo.put(DESCRIPTION_KEY, entry.description());
		dbo.put(URI_KEY, entry.uri());
		dbo.put(SEVERITY_KEY, entry.severity().toString());
		dbo.put(SOURCE_KEY, entry.classNameOfSource());
		
		TranslatorUtils.fromDateTime(dbo, TIMESTAMP_KEY, entry.timestamp());
		if (entry.exceptionSummary() != null) {
			dbo.put(EXCEPTION_KEY, toDbObject(entry.exceptionSummary()));
		}
		return dbo;
	}
	
	private DBObject toDbObject(ExceptionSummary summary) {
		DBObject dbo = new BasicDBObject();
		dbo.put(CAUSE_CLASS_NAME, summary.className());
		dbo.put(CAUSE_MESSAGE, summary.message());
		TranslatorUtils.fromList(dbo, summary.trace(), CAUSE_TRACE);
		
		ExceptionSummary current = summary.cause();
		DBObject parentDbo = dbo;
		
		while (current != null) {
			DBObject parent = toDbObject(current);
			parentDbo.put(CAUSE_PARENT, parent);
			current = current.cause();
			parentDbo = parent;
		}
		return dbo;
	}
	
	private ExceptionSummary exceptionSummaryfromDbObject(DBObject dbo) {
		List<String> trace = TranslatorUtils.toList(dbo, CAUSE_TRACE);
		String message = (String) dbo.get(CAUSE_MESSAGE);
		String className = (String) dbo.get(CAUSE_CLASS_NAME);
		return new ExceptionSummary(className, message, trace, causeFrom(dbo));
	}

	private ExceptionSummary causeFrom(DBObject dbo) {
		if (!dbo.containsField(CAUSE_PARENT)) {
			return null;
		}
		return exceptionSummaryfromDbObject((DBObject) dbo.get(CAUSE_PARENT));
	}

	private AdapterLogEntry fromDbObject(DBObject dbo) {
		Severity severity = Severity.valueOf((String) dbo.get(SEVERITY_KEY));
		AdapterLogEntry logEntry = new AdapterLogEntry((String) dbo.get(ID), severity, TranslatorUtils.toDateTime(dbo, TIMESTAMP_KEY))
			.withDescription((String) dbo.get(DESCRIPTION_KEY))
			.withUri((String) dbo.get(URI_KEY))
			.withSourceClassName((String) dbo.get(SOURCE_KEY));
		
		if (dbo.containsField(EXCEPTION_KEY)) {
			logEntry.withException(exceptionSummaryfromDbObject((DBObject) dbo.get(EXCEPTION_KEY)));
		}
		return logEntry;
	}

	@Override
	public AdapterLogEntry requireById(String id) {
		Maybe<DBObject> found = Maybe.firstElementOrNothing(where().idEquals(id).find(log));
		if (found.hasValue()) {
			return fromDbObject(found.requireValue());
		}
		throw new EntryExpiredException(id);
	}
}
