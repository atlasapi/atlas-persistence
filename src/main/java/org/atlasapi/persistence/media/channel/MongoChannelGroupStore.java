package org.atlasapi.persistence.media.channel;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.metabroadcast.common.persistence.mongo.MongoConstants.SINGLE;
import static com.metabroadcast.common.persistence.mongo.MongoConstants.UPSERT;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.persistence.mongo.MongoBuilders;
import com.metabroadcast.common.persistence.mongo.MongoConstants;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelGroup;

public class MongoChannelGroupStore implements ChannelGroupStore {

    private DBCollection channelGroups;
    private ChannelGroupTranslator translator = new ChannelGroupTranslator();

    public MongoChannelGroupStore(DatabasedMongo mongo) {
        this.channelGroups = mongo.collection("channelGroups");
    }
    
    @Override
    public Optional<ChannelGroup> channelGroupFor(Long id) {
        return Optional.fromNullable(translator.fromDBObject(channelGroups.findOne(id), null));
    }

    @Override
    public Iterable<ChannelGroup> channelGroupsFor(Iterable<Long> ids) {
        return transform(channelGroups.find(new BasicDBObject(MongoConstants.ID, new BasicDBObject(MongoConstants.IN,ids))));
    }

    public Iterable<ChannelGroup> transform(DBCursor dbos) {
        return Iterables.transform(dbos, new Function<DBObject, ChannelGroup>() {
            @Override
            public ChannelGroup apply(DBObject input) {
                return translator.fromDBObject(input, null);
            }
        });
    }

    @Override
    public Iterable<ChannelGroup> channelGroups() {
        return transform(channelGroups.find());
    }

    @Override
    public ChannelGroup store(ChannelGroup group) {
        checkNotNull(group);
        channelGroups.update(new BasicDBObject(MongoConstants.ID, checkNotNull(group.getId(), "Null ID")), translator.toDBObject(null, group), UPSERT, SINGLE);
        return group;
    }

    @Override
    public Iterable<ChannelGroup> channelGroupsFor(Channel channel) {
        return transform(channelGroups.find(MongoBuilders.where().fieldEquals(ChannelGroupTranslator.CHANNELS_KEY, channel.getId()).build()));
    }

}
