package org.atlasapi.media.channel;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.metabroadcast.common.persistence.mongo.MongoBuilders.where;
import static com.metabroadcast.common.persistence.mongo.MongoConstants.SINGLE;
import static com.metabroadcast.common.persistence.mongo.MongoConstants.UPSERT;

import java.util.Set;

import org.atlasapi.persistence.ids.MongoSequentialIdGenerator;
import org.atlasapi.persistence.media.entity.IdentifiedTranslator;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.persistence.mongo.MongoConstants;
import com.metabroadcast.common.persistence.mongo.MongoQueryBuilder;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class MongoChannelGroupStore implements ChannelGroupStore {

    private static final String CHANNEL_GROUP_CHANNEL_NUMBERING_ID_KEY = 
            ChannelGroupTranslator.CHANNEL_NUMBERINGS_KEY + "." + 
            ChannelNumberingTranslator.CHANNEL_KEY + "." + MongoConstants.ID;
    
    private DBCollection channelGroups;
    private static final String COLLECTION_NAME = "channelGroups";
    private MongoSequentialIdGenerator idGenerator;
    private ChannelGroupTranslator translator = new ChannelGroupTranslator();

    public MongoChannelGroupStore(DatabasedMongo mongo) {
        this.channelGroups = mongo.collection(COLLECTION_NAME);
        this.idGenerator = new MongoSequentialIdGenerator(mongo, COLLECTION_NAME);
    }
    
    @Override
    public Optional<ChannelGroup> channelGroupFor(Long id) {
        return Optional.fromNullable(translator.fromDBObject(channelGroups.findOne(id), null));
    }
    
    @Override
    public Optional<ChannelGroup> channelGroupFor(String canonicalUri) {
        return Optional.fromNullable(translator.fromDBObject(
                channelGroups.findOne(where().fieldEquals(IdentifiedTranslator.CANONICAL_URL, canonicalUri).build()), null));
    }

    @Override
    public Iterable<ChannelGroup> channelGroupsFor(Iterable<? extends Long> ids) {
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
    public ChannelGroup createOrUpdate(ChannelGroup group) {
        checkNotNull(group);
        if (group.getId() == null) {
            group.setId(idGenerator.generateRaw());
        } else {
            Optional<ChannelGroup> resolved = channelGroupFor(group.getId());

            Preconditions.checkState(resolved.isPresent(), "Channel Group not found to update");
            removeOldReferences(group, resolved.get());
        }
        
        if (group instanceof Region) {
            Region region = (Region) group;
            if (region.getPlatform() != null) {
                Optional<ChannelGroup> possiblePlatform = channelGroupFor(region.getPlatform());
                Preconditions.checkState(possiblePlatform.isPresent(), "Could not resolve platform with id " + region.getPlatform());
                Platform platform = (Platform) possiblePlatform.get();
                platform.addRegion(region);
                channelGroups.update(new BasicDBObject(MongoConstants.ID, platform.getId()), translator.toDBObject(null, platform), UPSERT, SINGLE);
            }
        }
        
        channelGroups.update(new BasicDBObject(MongoConstants.ID, group.getId()), translator.toDBObject(null, group), UPSERT, SINGLE);
        return group;
    }

    @Override
    public void deleteChannelGroupById(long channelGroupId) {
        checkNotNull(channelGroupId);
        channelGroups.remove(new BasicDBObject(MongoConstants.ID, channelGroupId));
    }

    private void removeOldReferences(ChannelGroup newGroup, ChannelGroup existingGroup) {
        if (newGroup instanceof Region) {
            Region existingRegion = (Region)existingGroup;
            Region newRegion = (Region)newGroup;

            if (existingRegion.getPlatform() != null) {
                if (newRegion.getPlatform() == null || !existingRegion.getPlatform().equals(newRegion.getPlatform())) {
                    Optional<ChannelGroup> maybeOldPlatform = channelGroupFor(existingRegion.getPlatform());
                    Preconditions.checkState(maybeOldPlatform.isPresent(), String.format("Platform with id %s not found for region with id %s", existingRegion.getPlatform(), existingRegion.getId()));

                    Platform oldPlatform = (Platform)maybeOldPlatform.get();
                    Set<Long> regions = Sets.newHashSet(oldPlatform.getRegions());
                    regions.remove(existingRegion.getId());
                    oldPlatform.setRegionIds(regions);
                    channelGroups.update(new BasicDBObject(MongoConstants.ID, oldPlatform.getId()), translator.toDBObject(null, oldPlatform), UPSERT, SINGLE);
                }
            }
        }
    }

    @Override
    public Iterable<ChannelGroup> channelGroupsFor(Channel channel) {
        return transform(channelGroups.find(where().fieldEquals(CHANNEL_GROUP_CHANNEL_NUMBERING_ID_KEY, channel.getId()).build()));
    }

    @Override
    public Optional<ChannelGroup> fromAlias(String alias) {
        MongoQueryBuilder query = new MongoQueryBuilder()
            .fieldEquals("aliases", alias);
        DBCursor cursor = channelGroups.find(query.build());
        if (Iterables.isEmpty(cursor)) {
            return Optional.absent();
        }
        return Optional.fromNullable(translator.fromDBObject(Iterables.getOnlyElement(cursor), null));
//        for (DBObject dbo : channelGroups.find()) {
//            ChannelGroup channelGroup = translator.fromDBObject(dbo, null);
//            for (String channelGroupAlias : channelGroup.getAliasUrls()) {
//                if (alias.equals(channelGroupAlias)) {
//                    return Optional.of(channelGroup);
//                }
//            }
//        }
//        return Optional.absent();
    }
}
