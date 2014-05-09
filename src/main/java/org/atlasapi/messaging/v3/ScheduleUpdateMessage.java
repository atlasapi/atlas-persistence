package org.atlasapi.messaging.v3;

import org.joda.time.DateTime;

import com.metabroadcast.common.queue.AbstractMessage;
import com.metabroadcast.common.time.Timestamp;


public class ScheduleUpdateMessage extends AbstractMessage {

    private final String source;
    private final String channel;
    private final DateTime updateStart;
    private final DateTime updateEnd;

    /**
     * @param messageId - unique message identifier
     * @param timestamp - message creation time
     * @param source - the Publisher.key()
     * @param channel - the channel id, lowercase-only encoded
     * @param updateStart - the start of the schedule update interval
     * @param updateEnd - the end of the schedule update interval
     */
    public ScheduleUpdateMessage(String messageId, Timestamp timestamp, String source, String channel, DateTime start, DateTime end) {
        super(messageId, timestamp);
        this.source = source;
        this.channel = channel;
        this.updateStart = start;
        this.updateEnd = end;
    }
    
    public String getSource() {
        return source;
    }

    public String getChannel() {
        return channel;
    }

    public DateTime getUpdateStart() {
        return updateStart;
    }
    
    public DateTime getUpdateEnd() {
        return updateEnd;
    }
}
