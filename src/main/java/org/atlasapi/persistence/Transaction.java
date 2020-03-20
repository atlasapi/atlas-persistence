package org.atlasapi.persistence;

import com.mongodb.client.ClientSession;

import javax.annotation.Nullable;
import java.io.Closeable;

/**
 * A layer of abstraction over a Mongo session to avoid having to expose the Mongo class as a return type
 */
public class Transaction implements Closeable {

    @Nullable private final ClientSession session;

    public Transaction(@Nullable ClientSession session) {
        this.session = session;
    }

    @Nullable
    public ClientSession getSession() {
        return session;
    }

    @Override
    public void close() {
        if (session != null) {
            session.close();
        }
    }
}
