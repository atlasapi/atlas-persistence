package org.atlasapi.persistence.content.schedule;

import static org.junit.Assert.*;

import org.joda.time.Interval;
import org.junit.Test;

public class ScheduleBroadcastFilterTest {

    private final Interval scheduleInterval = new Interval(1000, 2000);
    private final ScheduleBroadcastFilter filter = new ScheduleBroadcastFilter(scheduleInterval);
    private final Interval emptyInterval = new Interval(1000, 1000);
    private final ScheduleBroadcastFilter emptyFilter = new ScheduleBroadcastFilter(emptyInterval);
    
    @Test
    public void testFailsEntirelyBefore() {
        assertFalse(filter.apply(new Interval(200, 800)));
    }
    
    @Test
    public void testFailsEntirelyAfter() {
        assertFalse(filter.apply(new Interval(2200, 2800)));
    }
    
    @Test
    public void testPassesTotallyContained() {
        assertTrue(filter.apply(new Interval(1200, 1800)));
    }
    
    @Test
    public void testPassesOnlyStartContained() {
        assertTrue(filter.apply(new Interval(1200, 2800)));
    }
    
    @Test
    public void testPassesOnlyEndContained() {
        assertTrue(filter.apply(new Interval(200, 1800)));
    }
    
    @Test
    public void testPassesExactlyMatching() {
        assertTrue(filter.apply(new Interval(1000, 2000)));
    }
    
    @Test
    public void testPassesEntirelyOverlapping() {
        assertTrue(filter.apply(new Interval(200, 2800)));
    }

    @Test
    public void testFailsStartingAtEnd() {
        assertFalse(filter.apply(new Interval(scheduleInterval.getEndMillis(), 2800)));
    }

    @Test
    public void testFailsEndingAtStart() {
        assertFalse(filter.apply(new Interval(200, scheduleInterval.getStartMillis())));
    }

    @Test
    public void testFailsAbuttingStartOfEmptyInterval() {
        assertFalse(emptyFilter.apply(new Interval(200, 1000)));
    }

    @Test
    public void testPassesAbuttingEndOfEmptyInterval() {
        assertTrue(emptyFilter.apply(new Interval(1000, 1200)));
    }

    @Test
    public void testContainingEmptyInterval() {
        assertTrue(emptyFilter.apply(new Interval(200, 1200)));
    }
    
    @Test
    public void testFailsEntirelyBeforeEmptyInterval() {
        assertFalse(emptyFilter.apply(new Interval(200, 800)));
    }
    
    @Test
    public void testFailsEntirelyAfterEmptyInterval() {
        assertFalse(emptyFilter.apply(new Interval(2200, 2800)));
    }
    
}
