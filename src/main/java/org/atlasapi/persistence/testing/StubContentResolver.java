package org.atlasapi.persistence.testing;

import java.util.Map;

import org.atlasapi.media.common.Id;
import org.atlasapi.media.content.Content;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.content.ResolvedContent.ResolvedContentBuilder;

import com.google.common.collect.Maps;

public class StubContentResolver implements ContentResolver {

	public static ContentResolver RESOLVES_NOTHING = new StubContentResolver();
	
	private Map<String, Content> data = Maps.newHashMap();
	private Map<Id, Content> idData = Maps.newHashMap();
	
	public StubContentResolver respondTo(Content content) {
		data.put(content.getCanonicalUri(), content);
		idData.put(content.getId(), content);
		return this;
	}

	@Override
	public ResolvedContent findByCanonicalUris(Iterable<String> uris) {
		ResolvedContentBuilder builder = ResolvedContent.builder();
		for (String uri : uris) {
			Content r = data.get(uri);
            builder.put(r.getId(), r);
		}
		return builder.build();
	}

    @Override
    public ResolvedContent findByIds(Iterable<Id> ids) {
        ResolvedContentBuilder builder = ResolvedContent.builder();
        for (Id id : ids) {
            Content r = idData.get(id);
            builder.put(r.getId(), r);
        }
        return builder.build();
    }
}

