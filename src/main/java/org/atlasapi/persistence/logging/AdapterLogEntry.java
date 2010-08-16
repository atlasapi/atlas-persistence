package org.atlasapi.persistence.logging;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collection;
import java.util.List;
import java.util.UUID;

import org.joda.time.DateTime;

import com.google.common.base.Functions;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.metabroadcast.common.time.DateTimeZones;

public final class AdapterLogEntry {

	private final DateTime reportedAt;
	private final String id;
	
	private String uri;
	private ExceptionSummary e;
	private String description;
	private Class<?> clazz;
	
	public AdapterLogEntry(String id, DateTime timestamp) {
		this.id = checkNotNull(id);
		this.reportedAt = checkNotNull(timestamp);
	}
	
	public AdapterLogEntry() {
		this(UUID.randomUUID().toString(), new DateTime(DateTimeZones.UTC));
	}
	
	public AdapterLogEntry withUri(String uri) {
		this.uri = uri;
		return this;
	}
	
	public AdapterLogEntry withCause(Exception e) {
		return withException(new ExceptionSummary(e));
	}
	
	public AdapterLogEntry withException(ExceptionSummary exceptionSummary) {
		this.e = exceptionSummary;
		return this;
	}

	public AdapterLogEntry withDescription(String message) {
		this.description = message;
		return this;
	}
	
	public String description() {
		return description;
	}
	
	public String uri() {
		return uri;
	}
	
	public DateTime timestamp() {
		return reportedAt;
	}

	public AdapterLogEntry withSource(Class<?> clazz) {
		this.clazz = clazz;
		return this;
	}
	
	public Class<?> sourceOrDefault(Class<?> defaultClass) {
		if (clazz != null) {
			return clazz;
		}
		return defaultClass;
	}

	public String id() {
		return id;
	}
	
	@Override
	public int hashCode() {
		return id.hashCode();
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof AdapterLogEntry) {
			return id.equals(((AdapterLogEntry) obj).id);
		}
		return false;
	}
	
	@Override
	public String toString() {
		return Objects.toStringHelper(this).add("id", id).toString();
	}

	static class ExceptionSummary {
		
		private final String clazz;
		private final String message;
		private final List<String> trace;
		private final ExceptionSummary cause;
		
		public ExceptionSummary(Throwable e) {
			this(e.getClass().getName(), e.getMessage(), toStrings(e.getStackTrace()), toCause(e));
		}	

		private static ExceptionSummary toCause(Throwable e) {
			Throwable cause = e.getCause();
			if (cause != null) {
				return new ExceptionSummary(cause);
			}
			return null;
		}

		private static List<String> toStrings(StackTraceElement[] stackTrace) {
			return Lists.transform(ImmutableList.copyOf(stackTrace), Functions.toStringFunction());
		}

		public ExceptionSummary(String clazz, String message, List<String> trace, ExceptionSummary cause) {
			this.trace = trace;
			this.clazz = checkNotNull(clazz);
			this.message = message;
			this.cause = cause;
		}
		
		public String className() {
			return clazz;
		}
		
		public String message() {
			return message;
		}
		
		@Override
		public String toString() {
			return clazz + ":" + message;
		}

		public Collection<String> trace() {
			return trace;
		}

		public ExceptionSummary cause() {
			return cause;
		}
	} 

	public ExceptionSummary exceptionSummary() {
		return e;
	}
}
