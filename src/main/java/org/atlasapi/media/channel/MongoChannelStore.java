package org.atlasapi.media.channel;

import static com.metabroadcast.common.persistence.mongo.MongoBuilders.where;
import static com.metabroadcast.common.persistence.mongo.MongoConstants.ID;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.atlasapi.persistence.ids.MongoSequentialIdGenerator;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class MongoChannelStore implements ChannelResolver, ChannelWriter {

	private static final String COLLECTION = "channels";
	
	private DBCollection collection;
	private static final ChannelTranslator translator = new ChannelTranslator();

	private MongoSequentialIdGenerator idGenerator;
	private SubstitutionTableNumberCodec codec;
	private Cache<Long, Channel> cache;
	
	public MongoChannelStore(DatabasedMongo mongo) {
		this.collection = mongo.collection(COLLECTION);
		this.idGenerator = new MongoSequentialIdGenerator(mongo, COLLECTION);
		this.codec = new SubstitutionTableNumberCodec();
		this.cache = initializeCache();					
	}
	
	private Cache<Long, Channel> initializeCache() {
		
		Cache<Long, Channel> cache = CacheBuilder.newBuilder()
		.expireAfterWrite(10, TimeUnit.MINUTES)
		.build(new CacheLoader<Long, Channel>() {

			@Override
			public Channel load(Long id) throws Exception {
				DBObject dbo = collection.findOne(where().fieldEquals(ID, id).build());
				if(dbo == null) {
					throw new IllegalArgumentException(String.format("Channel ID %d not found", id));
				}
				return translator.fromDBObject(dbo, null);
			}
		});
		
		cache.asMap().putAll(Maps.uniqueIndex(Iterables.transform(collection.find(), DB_TO_CHANNEL_TRANSLATOR), new Function<Channel, Long>() {

			@Override
			public Long apply(Channel input) {
				return input.getId();
			}
				
		}));
		
		return cache;
	}
	
	@Override
	public Maybe<Channel> fromId(long id) {
		return Maybe.fromPossibleNullValue(translator.fromDBObject(collection.findOne(where().idEquals(id).build()), null));
	}

	@Override
	public Iterable<Channel> all() {
		return cache.asMap().values();
	}

    @Override
    public Iterable<Channel> forIds(Iterable<Long> ids) {
    	com.google.common.collect.ImmutableList.Builder<Channel> channels = ImmutableList.builder();
    	for(Long id: ids) {
    		try {
    			channels.add(cache.get(id));
    		}
    		catch(ExecutionException e) {
    			Throwables.propagate(e);
    		}
    	}
    	return channels.build();
        
    }

	@Override
	public Maybe<Channel> fromUri(final String uri) {
		
		return Maybe.fromPossibleNullValue(Iterables.getFirst(Iterables.filter(cache.asMap().values(), new Predicate<Channel>() {

			@Override
			public boolean apply(Channel input) {
				return input.getCanonicalUri().equals(uri);
			}
			
		}), null));
	}
	
	@Override
	public Maybe<Channel> fromKey(final String key) {
		return Maybe.fromPossibleNullValue(Iterables.getFirst(Iterables.filter(cache.asMap().values(), new Predicate<Channel>() {

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
		cache.asMap().put(channel.getId(), channel);
	}


    @Override
    public Map<String, Channel> forAliases(String aliasPrefix) {
        final Pattern prefixPattern = Pattern.compile(String.format("^%s", Pattern.quote(aliasPrefix)));

        Builder<String, Channel> channelMap = ImmutableMap.builder();
        for (Channel channel : cache.asMap().values()) {
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
