package org.atlasapi.persistence.messaging.mongo;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.joda.time.DateTime;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import java.io.IOException;
import org.atlasapi.messaging.Message;
import org.atlasapi.persistence.messaging.MessageStore;
import org.atlasapi.serialization.json.JsonFactory;

public class MongoMessageStore implements MessageStore {

    private final static String MESSAGES = "messages";
    //
    private final ObjectMapper mapper = JsonFactory.makeJsonMapper();
    private final DBCollection messages;

    public MongoMessageStore(DatabasedMongo mongo) {
        this.messages = mongo.collection(MESSAGES);
    }

    @Override
    public void add(Message message) {
        try {
            messages.save((DBObject) JSON.parse(mapper.writeValueAsString(message)));
        } catch (IOException ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }

    @Override
    public Iterable<Message> get(DateTime from, DateTime to) {
        DBObject query = new BasicDBObject();
        query.put("timestamp", new BasicDBObject("$gte", from.getMillis()).append("$lt", to.getMillis()));
        DBObject keys = new BasicDBObject();
        keys.put("_id", 0);
        return Iterables.transform(messages.find(query, keys).sort(new BasicDBObject("timestamp", 1)), new Function<DBObject, Message>() {

            @Override
            public Message apply(DBObject input) {
                try {
                    return mapper.readValue(JSON.serialize(input), Message.class);
                } catch (IOException ex) {
                    throw new RuntimeException(ex.getMessage(), ex);
                }
            }
        });
    }
}
