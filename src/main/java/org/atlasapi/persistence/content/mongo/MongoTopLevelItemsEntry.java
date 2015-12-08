package org.atlasapi.persistence.content.mongo;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.ReadPreference;
import static com.metabroadcast.common.persistence.mongo.MongoBuilders.where;
import static com.metabroadcast.common.persistence.mongo.MongoConstants.ID;


import org.atlasapi.media.entity.Item;
import org.atlasapi.persistence.media.entity.ItemTranslator;

public class MongoTopLevelItemsEntry {

    private final DBCollection dbCollection;
    private final ReadPreference readPreference;
    private final ItemTranslator itemTranslator;

    public static String EVENTS = "events._id";

    public MongoTopLevelItemsEntry(DBCollection dbCollection, ReadPreference readPreference) {
        this.dbCollection = dbCollection;
        this.readPreference = readPreference;
        this.itemTranslator = new ItemTranslator(new SubstitutionTableNumberCodec());
    }

    public Iterable<String> getItemUrisForEventIds(Iterable<Long> eventIds) {
        return ImmutableSet.copyOf(Iterables.transform(
                ImmutableSet.copyOf(Iterables.transform(find(eventIds), TO_ITEM)), TO_URI));
    }

    private Iterable<DBObject> find(Iterable<Long> eventIds) {
        return dbCollection.find(where().longFieldIn(EVENTS, eventIds).build())
                .setReadPreference(readPreference);
    }

    private Function<DBObject, Item> TO_ITEM = new Function<DBObject, Item>() {
         @Override
         public Item apply(DBObject dbObject) {
            return itemTranslator.fromDB(dbObject);
        }
    };

    private Function<Item, String> TO_URI = new Function<Item, String>() {
        @Override public String apply(Item item) {
            return item.getCanonicalUri();
        }
    };

}
