package org.atlasapi.persistence.media.entity;

import java.util.List;

import org.atlasapi.media.entity.Channel;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.ScheduleEntry;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import com.google.common.collect.ImmutableList;
import com.metabroadcast.common.persistence.mongo.MongoConstants;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class ScheduleEntryTranslator {
    
    private final ItemTranslator itemTranslator = new ItemTranslator();

    public DBObject toDb(ScheduleEntry entry) {
        DBObject dbObject = new BasicDBObject();
        dbObject.put(MongoConstants.ID, entry.toKey());
        dbObject.put("publisher", entry.publisher().key());
        dbObject.put("channel", entry.channel().key());
        TranslatorUtils.fromDateTime(dbObject, "intervalStart", entry.interval().getStart());
        TranslatorUtils.fromDateTime(dbObject, "intervalEnd", entry.interval().getEnd());
        
        BasicDBList items = new BasicDBList();
        for (Item item: entry.items()) {
            items.add(itemTranslator.toDBObject(null, item));
        }
        dbObject.put("content", items);
        
        return dbObject;
    }
    
    public List<DBObject> toDbObjects(Iterable<ScheduleEntry> entries) {
        ImmutableList.Builder<DBObject> dbObjects = ImmutableList.builder();
        for (ScheduleEntry entry: entries) {
            dbObjects.add(toDb(entry));
        }
        return dbObjects.build();
    }
    
    @SuppressWarnings("unchecked")
    public ScheduleEntry fromDb(DBObject object) {
        Publisher publisher = Publisher.fromKey((String) object.get("publisher")).requireValue();
        Channel channel = Channel.fromKey((String) object.get("channel")).requireValue();
        DateTime start = TranslatorUtils.toDateTime(object, "intervalStart");
        DateTime end = TranslatorUtils.toDateTime(object, "intervalEnd");
        Interval interval = new Interval(start, end);
        
        ImmutableList.Builder<Item> items = ImmutableList.builder();
        List<DBObject> dbItems = (List) object.get("content");
        for (DBObject item: dbItems) {
            items.add(itemTranslator.fromDBObject(item, null));
        }
        return new ScheduleEntry(interval, channel, publisher, items.build());
    }
    
    public List<ScheduleEntry> fromDbObjects(Iterable<DBObject> objects) {
        ImmutableList.Builder<ScheduleEntry> entries = ImmutableList.builder();
        for (DBObject dbObject: objects) {
            entries.add(fromDb(dbObject));
        }
        return entries.build();
    }
}
