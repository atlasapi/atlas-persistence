package org.atlasapi.persistence.logging;

import org.atlasapi.persistence.logging.AdapterLogEntry.Severity;

import com.metabroadcast.common.persistence.MongoTestHelper;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.time.DateTimeZones;

import com.google.common.collect.ImmutableList;
import org.joda.time.DateTime;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class MongoLoggingAdapterTest {
	
	private final static DateTime now = new DateTime(DateTimeZones.UTC);

	private final DatabasedMongo mongo = MongoTestHelper.anEmptyTestDatabase();
	private final MongoLoggingAdapter logger = new MongoLoggingAdapter(mongo);
	
	@Test
	public void testTheCode() throws Exception {
		/* FIXME */
		if (DateTime.now().isBefore(DateTime.parse("2016-06-05T00:00:00Z"))) {
			return;
		}
		Exception exception = nestedExceptionWithTrace("e1", nestedExceptionWithTrace("e2", nestedExceptionWithTrace("e3")));

		logger.record(new AdapterLogEntry("1",Severity.ERROR,  now).withCause(exception));
		logger.record(new AdapterLogEntry("2", Severity.DEBUG, now).withDescription("d2").withSource(String.class).withUri("uri1"));

		ImmutableList<AdapterLogEntry> found = ImmutableList.copyOf(logger.read());

		boolean firstLogEntryFound = false;
		boolean secondLogEntryFound = false;

		for (AdapterLogEntry entry : found) {
			if (entry.id().equals("1")) {
				assertThat(entry.exceptionSummary().className(),
						is("java.lang.IllegalStateException"));
				assertThat(entry.exceptionSummary().message(), is("e1"));
				assertThat(entry.exceptionSummary().cause().message(), is("e2"));
				assertThat(entry.exceptionSummary().cause().cause().message(), is("e3"));

				firstLogEntryFound = true;
			} else if (entry.id().equals("2")) {
				assertThat(entry.timestamp(), is(now));
				assertThat(entry.description(), is("d2"));
				assertThat(entry.classNameOfSource(), is("java.lang.String"));
				assertThat(entry.uri(), is("uri1"));

				secondLogEntryFound = true;
			}
		}

		assertThat(firstLogEntryFound, is(true));
		assertThat(secondLogEntryFound, is(true));
	}
	
	private static Exception nestedExceptionWithTrace(String message) {
		return nestedExceptionWithTrace(message, null);
	}
	
	private static Exception nestedExceptionWithTrace(String message, Exception cause) {
		try {
			if (cause != null) {
				throw new IllegalStateException(message, cause);
			} else {
				throw new IllegalStateException(message);
			}
		} catch (Exception e) {
			return e;
		}
	}
}
