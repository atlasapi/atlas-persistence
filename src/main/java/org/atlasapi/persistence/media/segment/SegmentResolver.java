package org.atlasapi.persistence.media.segment;

import java.util.Map;

import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.common.base.Maybe;
import org.atlasapi.media.segment.Segment;
import org.atlasapi.media.segment.SegmentRef;

public interface SegmentResolver {

    Map<SegmentRef,Maybe<Segment>> resolveById(Iterable<SegmentRef> identifier);
    
    Maybe<Segment> resolveForSource(Publisher source, String sourceId); 
    
}
