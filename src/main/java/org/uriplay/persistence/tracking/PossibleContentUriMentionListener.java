package org.uriplay.persistence.tracking;

import org.uriplay.persistence.tracking.ContentMention;

public interface PossibleContentUriMentionListener {

	void mentioned(ContentMention mention);
}
