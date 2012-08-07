package org.atlasapi.persistence.media.channel;

import static com.metabroadcast.common.persistence.mongo.MongoBuilders.where;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;

import org.atlasapi.persistence.ids.MongoSequentialIdGenerator;
import org.joda.time.Duration;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.caching.BackgroundComputingValue;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import org.atlasapi.media.channel.Channel;

public class MongoChannelStore implements ChannelResolver, ChannelWriter {

	public static final String COLLECTION = "channels";
	
	private final DBCollection collection;
	private static final ChannelTranslator translator = new ChannelTranslator();

	private MongoSequentialIdGenerator idGenerator;
	private SubstitutionTableNumberCodec codec;
	private BackgroundComputingValue<Map<Long, Channel>> channels;
	
	public MongoChannelStore(DatabasedMongo mongo) {
		this(mongo, Duration.standardMinutes(15));
	}
	
	public MongoChannelStore(DatabasedMongo mongo, Duration cacheExpiry) {
		this.collection = mongo.collection(COLLECTION);
		this.idGenerator = new MongoSequentialIdGenerator(mongo, COLLECTION);
		this.codec = new SubstitutionTableNumberCodec();
		this.channels = new BackgroundComputingValue<Map<Long, Channel>>(cacheExpiry, new Callable<Map<Long, Channel>>() {

			@Override
			public Map<Long, Channel> call() throws Exception {
				return getChannels();
			}
		});
		channels.start(getChannels());
	}
	
	private Map<Long, Channel> getChannels() {
		
		return ImmutableMap.copyOf(Maps.uniqueIndex(Iterables.transform(collection.find(), DB_TO_CHANNEL_TRANSLATOR), new Function<Channel, Long>() {

			@Override
			public Long apply(Channel input) {
				return input.getId();
			}
			
		}));
	}
	@Override
	public Maybe<Channel> fromId(long id) {
		return Maybe.fromPossibleNullValue(translator.fromDBObject(collection.findOne(where().idEquals(id).build()), null));
	}

	@Override
	public Iterable<Channel> all() {
		return channels.get().values();
	}

    @Override
    public Iterable<Channel> forIds(Iterable<Long> ids) {
    	com.google.common.collect.ImmutableList.Builder<Channel> returnedChannels = ImmutableList.builder();
    	for(Long id: ids) {
    		returnedChannels.add(channels.get().get(id));
    	}
    	return returnedChannels.build();
        
    }

	@Override
	public Maybe<Channel> fromUri(final String uri) {
		
		Maybe<Channel> channel = Maybe.fromPossibleNullValue(Iterables.getFirst(Iterables.filter(channels.get().values(), new Predicate<Channel>() {

			@Override
			public boolean apply(Channel input) {
				return input.getCanonicalUri().equals(uri);
			}
			
		}), null));
		
		if(channel.isNothing()) {
			System.out.println(String.format("Cannot find channel for URI %s", uri));
		}
		return channel;
	}
	
	@Override
	public Maybe<Channel> fromKey(final String key) {
		return Maybe.fromPossibleNullValue(Iterables.getFirst(Iterables.filter(channels.get().values(), new Predicate<Channel>() {

			@Override
			public boolean apply(Channel input) {
				return input.key().equals(key);
			}
			
		}), null));
	}

	@Override
	public void write(Channel channel) {
		if(channel.getId() == null) {
			// TODO: skip ids of legacy channel names
			channel.setId(codec.decode(idGenerator.generate()).longValue());
		}
		collection.insert(translator.toDBObject(null, channel));
	}


    @Override
    public Map<String, Channel> forAliases(String aliasPrefix) {
        final Pattern prefixPattern = Pattern.compile(String.format("^%s", Pattern.quote(aliasPrefix)));

        Builder<String, Channel> channelMap = ImmutableMap.builder();
        for (Channel channel : channels.get().values()) {
            for (String alias : Iterables.filter(channel.getAliases(), Predicates.contains(prefixPattern))) {
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

}
