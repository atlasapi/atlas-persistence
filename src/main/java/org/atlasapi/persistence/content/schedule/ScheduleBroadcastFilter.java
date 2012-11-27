package org.atlasapi.persistence.content.schedule;

import org.joda.time.Interval;

import com.google.common.base.Predicate;

/**
 * Predicate for filtering broadcast intervals in a schedule interval. Valid
 * broadcast intervals either entirely or partially overlap with the schedule
 * interval or, if the schedule interval is an instance, abut its end.
 * 
 * Along with abutting the end of an empty interval, the following cases are
 * covered:
 * 
 * <pre>
 * Schedule Interval:          SS-------------------SE 
 * Totally Contained:                bs------be 
 * Only Start Contained:                        bs-------be 
 * Only End Contained:     bs------be 
 * Exact match:                bs-------------------be 
 * Entirely Overlapping:   bs----------------------------be
 * </pre>
 * 
 * @author Fred van den Driessche (fred@metabroadcast.com)
 * 
 */
public final class ScheduleBroadcastFilter implements Predicate<Interval> {

    private final Interval scheduleInterval;
    private final boolean scheduleIntervalEmpty;

    public ScheduleBroadcastFilter(Interval scheduleInterval) {
        this.scheduleInterval = scheduleInterval;
        this.scheduleIntervalEmpty = scheduleInterval.toDuration().getMillis() == 0;
    }

    /*
     * The first clause of the disjunction covers the general overlapping case
     * whether or not the schedule interval is empty. The other case that to
     * cover is a broadcast which starts at the end of an empty interval.
     */
    @Override
    public boolean apply(Interval broadcastInterval) {
        return scheduleInterval.overlaps(broadcastInterval)
            || abutsEndOfEmptyScheduleInterval(broadcastInterval);
    }

    private boolean abutsEndOfEmptyScheduleInterval(Interval broadcastInterval) {
        return scheduleIntervalEmpty
            && scheduleInterval.abuts(broadcastInterval)
            && scheduleInterval.isBefore(broadcastInterval);
    }

}