package org.atlasapi.media.channel;

import com.google.common.collect.Sets;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.atlasapi.equiv.ChannelRef;
import org.atlasapi.media.entity.Publisher;

import java.util.List;
import java.util.Set;

public class ChannelRefTranslator {

    public DBObject toDBObject(ChannelRef channelRef) {

        DBObject dbObject = new BasicDBObject();

        TranslatorUtils.from(dbObject, "id", channelRef.getId());
        TranslatorUtils.from(dbObject, "uri", channelRef.getUri());
        TranslatorUtils.from(dbObject, "publisher", channelRef.getPublisher().key());

        return dbObject;
    }

    public ChannelRef fromDBObject(DBObject dbObject) {

        return ChannelRef.create(
                TranslatorUtils.toLong(dbObject, "id"),
                TranslatorUtils.toString(dbObject, "uri"),
                Publisher.fromKey(TranslatorUtils.toString(dbObject, "publisher")).requireValue()
        );

    }

    public void fromChannelRefSet(DBObject dbObject, String key, Set<ChannelRef> channelRefs) {

        BasicDBList values = new BasicDBList();
        channelRefs.forEach(ref -> values.add(toDBObject(ref)));

        dbObject.put(key, values);
    }

    @SuppressWarnings("unchecked")
    public Set<ChannelRef> toChannelRefSet(DBObject dbObject, String key) {
        Set<ChannelRef> channelRefs = Sets.newHashSet();

        if (dbObject.containsField(key)) {
            ((List<DBObject>)dbObject.get(key)).forEach(element ->
                    channelRefs.add(fromDBObject(element))
            );
        }

        return channelRefs;
    }

}
