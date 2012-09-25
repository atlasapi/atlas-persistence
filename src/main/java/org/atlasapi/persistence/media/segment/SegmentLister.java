package org.atlasapi.persistence.media.segment;

import org.atlasapi.media.segment.Segment;

/**
 *
 */
public interface SegmentLister {

    Iterable<Segment> segments();
}
