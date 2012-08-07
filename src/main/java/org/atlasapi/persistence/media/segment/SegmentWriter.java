package org.atlasapi.persistence.media.segment;

import org.atlasapi.media.segment.Segment;

public interface SegmentWriter {

    Segment write(Segment segment);
    
}
