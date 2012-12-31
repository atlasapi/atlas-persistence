package org.atlasapi.persistence.messaging;

import org.atlasapi.messaging.Message;
import org.joda.time.DateTime;

import com.google.common.base.Optional;

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
     * @param source 
     * @return
     */
    Iterable<Message> get(DateTime from, DateTime to, Optional<String> source);
}
