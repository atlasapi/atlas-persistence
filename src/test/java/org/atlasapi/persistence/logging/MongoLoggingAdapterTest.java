package org.atlasapi.persistence.logging;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import org.joda.time.DateTime;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.metabroadcast.common.persistence.MongoTestHelper;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.time.DateTimeZones;

public class MongoLoggingAdapterTest {
	
	private final static DateTime now = new DateTime(DateTimeZones.UTC);

	private final DatabasedMongo mongo = MongoTestHelper.anEmptyTestDatabase();
	private final MongoLoggingAdapter logger = new MongoLoggingAdapter(mongo);
	
	@Test
	public void testTheCode() throws Exception {
		
		Exception exception = nestedExceptionWithTrace("e1", nestedExceptionWithTrace("e2", nestedExceptionWithTrace("e3")));
		
		logger.record(new AdapterLogEntry("1", now).withCause(exception));
		logger.record(new AdapterLogEntry("2", now).withDescription("d2"));
		
		ImmutableList<AdapterLogEntry> found = ImmutableList.copyOf(logger.read());
		
		AdapterLogEntry latest = found.get(0);
		
		assertThat(latest.id(), is("2"));
		assertThat(latest.timestamp(), is(now));
		assertThat(latest.description(), is("d2"));
		
		AdapterLogEntry oldest = found.get(1);
		assertThat(oldest.exceptionSummary().className(), is("java.lang.IllegalStateException"));
		assertThat(oldest.exceptionSummary().message(), is("e1"));
		assertThat(oldest.exceptionSummary().cause().message(), is("e2"));
		assertThat(oldest.exceptionSummary().cause().cause().message(), is("e3"));
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
