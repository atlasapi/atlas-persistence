package org.atlasapi.persistence.messaging.mongo;

import static com.metabroadcast.common.persistence.mongo.MongoConstants.GREATER_THAN_OR_EQUAL_TO;
import static com.metabroadcast.common.persistence.mongo.MongoConstants.LESS_THAN;

import java.io.IOException;

import org.atlasapi.messaging.Message;
import org.atlasapi.persistence.messaging.MessageStore;
import org.atlasapi.serialization.json.JsonFactory;
import org.joda.time.DateTime;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;

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
    public Iterable<Message> get(DateTime from, DateTime to, Optional<String> source) {
        DBObject query = new BasicDBObject();
        query.put("timestamp", ImmutableMap.of(
            GREATER_THAN_OR_EQUAL_TO, from.getMillis(),
            LESS_THAN, to.getMillis()
        ));
        if (source.isPresent()) {
            query.put("entitySource", source.get());
        }
        //fixes deserialization problem.
        DBObject keys = new BasicDBObject("_id", 0);
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
