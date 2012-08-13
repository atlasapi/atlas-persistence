package org.atlasapi.persistence.messaging;

import org.atlasapi.messaging.Message;
import org.joda.time.DateTime;

public interface MessageStore {

    /**
     *
     * @param message
     */
    void add(Message message);

    /**
     *
     * @param from
     * @param to
     * @return
     */
    Iterable<Message> get(DateTime from, DateTime to);
}
