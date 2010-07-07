package org.atlasapi.persistence.tracking;

import org.joda.time.DateTime;

public class ContentMention {
	private String uri;
	private String externalRef;
	private TrackingSource trackingSource;
	
	private Long mentionedAt;

	public ContentMention(String uri, TrackingSource source, String externalRef, DateTime when) {
		if (uri == null) {
			throw new IllegalArgumentException("uri cannot be null");
		}
		if (externalRef == null) {
			throw new IllegalArgumentException("external ref cannot be null");
		}
		this.uri = uri;
		this.trackingSource = source;
		this.externalRef = externalRef;
		if (when != null) {
		    this.mentionedAt = when.getMillis();
		}
	}
	
	public ContentMention() {}

	public String uri() {
		return uri;
	}

	public String externalRef() {
		return externalRef;
	}
	
	public DateTime mentionedAt() {
	    if (mentionedAt != null) {
		return new DateTime(mentionedAt);
	    } else {
	        return null;
	    }
	}

	public TrackingSource source() {
		return trackingSource;
	}
	
	@Override
	public int hashCode() {
		return externalRef.hashCode();
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof ContentMention) {
			ContentMention other = (ContentMention) obj;
			return uri.equals(other.uri) && externalRef.equals(other.externalRef) && trackingSource.equals(other.trackingSource);
		}
		return false;
	}
}
