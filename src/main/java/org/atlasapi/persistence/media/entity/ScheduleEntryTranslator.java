package org.atlasapi.persistence.media.entity;

import java.util.List;

import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Channel;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.ScheduleEntry;
import org.atlasapi.media.entity.ScheduleEntry.ItemRefAndBroadcast;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.persistence.mongo.MongoConstants;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class ScheduleEntryTranslator {
    
    private static final String BROADCAST_KEY = "broadcast";
    private static final String ITEM_URI_KEY = "itemUri";
    private static final String ITEM_REFS_AND_BROADCAST_KEY = "itemsAndBroadcasts";
    
    private final BroadcastTranslator broadcastTranslator = new BroadcastTranslator();

    public DBObject toDb(ScheduleEntry entry) {
        DBObject dbObject = new BasicDBObject();
        dbObject.put(MongoConstants.ID, entry.toKey());
        dbObject.put("publisher", entry.publisher().key());
        dbObject.put("channel", entry.channel().key());
        TranslatorUtils.fromDateTime(dbObject, "intervalStart", entry.interval().getStart());
        TranslatorUtils.fromDateTime(dbObject, "intervalEnd", entry.interval().getEnd());
        
        dbObject.put(ITEM_REFS_AND_BROADCAST_KEY, Iterables.transform(entry.getItemRefsAndBroadcasts(), new Function<ItemRefAndBroadcast, DBObject>() {
            @Override
            public DBObject apply(ItemRefAndBroadcast input) {
                BasicDBObject dbo = new BasicDBObject(ITEM_URI_KEY, input.getItemUri());
                dbo.put(BROADCAST_KEY, broadcastTranslator.toDBObject(input.getBroadcast()));
                return dbo;
            }
        }));
        
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
        
        Iterable<DBObject> itemsAndBroacasts = (Iterable<DBObject>) object.get(ITEM_REFS_AND_BROADCAST_KEY);
        
        if (itemsAndBroacasts == null) {
            return new ScheduleEntry(interval, channel, publisher, ImmutableList.<ItemRefAndBroadcast>of());
        }
        
        return new ScheduleEntry(interval, channel, publisher, Iterables.transform(itemsAndBroacasts, new Function<DBObject, ItemRefAndBroadcast>() {
            @Override
            public ItemRefAndBroadcast apply(DBObject dbo) {
               Broadcast broadcast = broadcastTranslator.fromDBObject((DBObject) dbo.get(BROADCAST_KEY));
               return new ItemRefAndBroadcast((String) dbo.get(ITEM_URI_KEY), broadcast);
            }
        }));
    }
    
    public List<ScheduleEntry> fromDbObjects(Iterable<DBObject> objects) {
        ImmutableList.Builder<ScheduleEntry> entries = ImmutableList.builder();
        for (DBObject dbObject: objects) {
            entries.add(fromDb(dbObject));
        }
        return entries.build();
    }
    
    public DBObject toIndex() {
        return new BasicDBObject(MongoConstants.ID, 1).append("channel", 1).append("publisher", 1);
    }
}
