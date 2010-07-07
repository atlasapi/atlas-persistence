package org.atlasapi.persistence.tracking;

import org.atlasapi.persistence.tracking.ContentMention;

public interface PossibleContentUriMentionListener {

	void mentioned(ContentMention mention);
}
