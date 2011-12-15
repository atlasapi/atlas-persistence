package org.atlasapi.persistence;

import org.atlasapi.media.segment.SegmentResolver;
import org.atlasapi.media.segment.SegmentWriter;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.people.ItemsPeopleWriter;
import org.atlasapi.persistence.shorturls.ShortUrlSaver;

public interface ContentPersistenceModule {

	ContentWriter contentWriter();
	
	ItemsPeopleWriter itemsPeopleWriter();
	
	ContentResolver contentResolver();

	ShortUrlSaver shortUrlSaver();
	
	SegmentWriter segmentWriter();
	
	SegmentResolver segmentResolver();
	
}
