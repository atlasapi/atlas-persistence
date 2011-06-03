package org.atlasapi.persistence.content.mongo;

import java.util.Map;

import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.persistence.content.ContentResolver;

import com.metabroadcast.common.base.Maybe;

public class MongoContentResolver implements ContentResolver {

    @Override
    public Map<LookupRef, Maybe<Identified>> findByLookupRefs(Iterable<LookupRef> lookupRefs) {
        // TODO Auto-generated method stub
        return null;
    }

}
