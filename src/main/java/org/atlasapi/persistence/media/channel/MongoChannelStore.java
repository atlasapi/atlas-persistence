package org.atlasapi.persistence.media.channel;

import static com.metabroadcast.common.persistence.mongo.MongoBuilders.where;
import static com.metabroadcast.common.persistence.mongo.MongoConstants.SINGLE;
import static com.metabroadcast.common.persistence.mongo.MongoConstants.UPSERT;
import static org.atlasapi.persistence.media.channel.ChannelTranslator.KEY;
import static org.atlasapi.persistence.media.entity.IdentifiedTranslator.CANONICAL_URL;

import java.util.Map;
import java.util.regex.Pattern;

import org.atlasapi.persistence.ids.MongoSequentialIdGenerator;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.persistence.mongo.MongoConstants;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelGroup;
import org.atlasapi.media.channel.ChannelNumbering;

public class MongoChannelStore implements ChannelStore {

	public static final String COLLECTION = "channels";
	
	private final DBCollection collection;
	private static final ChannelTranslator translator = new ChannelTranslator();

	private MongoSequentialIdGenerator idGenerator;
	private SubstitutionTableNumberCodec codec;
	
	private final ChannelGroupResolver channelGroupResolver;
	private final ChannelGroupWriter channelGroupWriter;
	
	public MongoChannelStore(DatabasedMongo mongo, ChannelGroupResolver channelGroupResolver, ChannelGroupWriter channelGroupWriter) {
		this.channelGroupResolver = channelGroupResolver;
        this.channelGroupWriter = channelGroupWriter;
        this.collection = mongo.collection(COLLECTION);
		this.idGenerator = new MongoSequentialIdGenerator(mongo, COLLECTION);
		this.codec = new SubstitutionTableNumberCodec();
	}
	
	@Override
	public Maybe<Channel> fromId(long id) {
		return Maybe.fromPossibleNullValue(translator.fromDBObject(collection.findOne(where().idEquals(id).build()), null));
	}

	@Override
	public Iterable<Channel> all() {
		return Iterables.transform(collection.find(), DB_TO_CHANNEL_TRANSLATOR);
	}

    @Override
    public Iterable<Channel> forIds(Iterable<Long> ids) {
        return Iterables.transform(collection.find(where().longIdIn(ids).build()), DB_TO_CHANNEL_TRANSLATOR);
    }

	@Override
	public Maybe<Channel> fromUri(final String uri) {
	    return Maybe.fromPossibleNullValue(translator.fromDBObject(collection.findOne(where().fieldEquals(CANONICAL_URL, uri).build()), null));
	}
	
	@Override
	public Maybe<Channel> fromKey(final String key) {
	    return Maybe.fromPossibleNullValue(translator.fromDBObject(collection.findOne(where().fieldEquals(KEY, key).build()), null));
	}

	@Override
	public Channel write(Channel channel) {
		if(channel.getId() == null) {
			// TODO: skip ids of legacy channel names
			channel.setId(codec.decode(idGenerator.generate()).longValue());
		}

		if (channel.parent() != null) {
		    // channel is child, add reference to it to the parent channel
		    Maybe<Channel> maybeParent = fromId(channel.parent());
		    
		    if (maybeParent.isNothing()) {
	            throw new IllegalStateException(String.format("Parent channel with id %s not found for channel with id %s", channel.parent(), channel.getId()));
	        }
		    
		    Channel parent = maybeParent.requireValue();
		    parent.addVariation(channel.getId());
		    write(parent);
		}
		
		for (ChannelNumbering channelNumbering : channel.channelNumbers()) {
		    // fetch channelgroup
		    Optional<ChannelGroup> maybeGroup = channelGroupResolver.channelGroupFor(channelNumbering.getChannelGroup());
		    if (!maybeGroup.isPresent()) {
		        throw new IllegalStateException(String.format("ChannelGroup with id %s not found for channel with id %s", channelNumbering.getChannelGroup(), channel.getId()));
		    }
		    ChannelGroup group = maybeGroup.get();
		    // add channelNumbering
		    group.addChannelNumbering(channelNumbering);
		    // write channel numbering
		    channelGroupWriter.store(group);
		}
		
		collection.update(new BasicDBObject(MongoConstants.ID, channel.getId()), translator.toDBObject(null, channel), UPSERT, SINGLE);
		
		return channel;
	}


    @Override
    public Map<String, Channel> forAliases(String aliasPrefix) {
        final Pattern prefixPattern = Pattern.compile(String.format("^%s", Pattern.quote(aliasPrefix)));

        Iterable<Channel> channels = all();

        Builder<String, Channel> channelMap = ImmutableMap.builder();
        for (Channel channel : channels) {
            // TODO new aliases
            for (String alias : Iterables.filter(channel.getAliasUrls(), Predicates.contains(prefixPattern))) {
                channelMap.put(alias, channel);
            }
        }
        return channelMap.build();
    }
    
	private final Function<DBObject, Channel> DB_TO_CHANNEL_TRANSLATOR = new Function<DBObject, Channel>() {

		@Override
		public Channel apply(DBObject input) {
			return translator.fromDBObject(input, null);
		}
	};

    @Override
    public Maybe<Channel> forAlias(String alias) {
        Iterable<Channel> channels = all();
        for (Channel channel : channels) {
            // TODO new aliases
            for (String channelAlias : channel.getAliasUrls()) {
                if (alias.equals(channelAlias)) {
                    return Maybe.just(channel);
                }
            }
        }
        return Maybe.nothing();
    }

}
