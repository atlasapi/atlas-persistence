package org.atlasapi.persistence;

import com.mongodb.client.ClientSession;

import javax.annotation.Nullable;
import java.io.Closeable;

/**
 * A layer of abstraction over a Mongo session to avoid having to expose the Mongo class as a return type
 */
public class Transaction implements Closeable {

    @Nullable private final ClientSession session;

    private Transaction(@Nullable ClientSession session) {
        this.session = session;
    }

    public static Transaction of(@Nullable ClientSession session) {
        return new Transaction(session);
    }


    private static final Transaction NONE = new Transaction(null);
    public static Transaction none() {
        return NONE;
    }

    @Nullable
    public ClientSession getSession() {
        return session;
    }

    public void commit() {
        if (session != null) {
            session.commitTransaction();
        }
    }

    @Override
    public void close() {
        if (session != null) {
            session.close();
        }
    }
}
