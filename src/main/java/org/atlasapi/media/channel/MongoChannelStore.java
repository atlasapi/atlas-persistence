package org.atlasapi.media.channel;

import static com.metabroadcast.common.persistence.mongo.MongoBuilders.where;
import static com.metabroadcast.common.persistence.mongo.MongoConstants.ID;
import static com.metabroadcast.common.persistence.mongo.MongoConstants.IN;

import java.util.Map;
import java.util.regex.Pattern;

import org.atlasapi.media.channel.Channel;
import org.atlasapi.persistence.ids.MongoSequentialIdGenerator;
import org.atlasapi.persistence.media.entity.IdentifiedTranslator;

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class MongoChannelStore implements ChannelResolver, ChannelWriter {

	private static final String COLLECTION = "channels";
	
	private DBCollection collection;
	private static final ChannelTranslator translator = new ChannelTranslator();

	private MongoSequentialIdGenerator idGenerator;
	private SubstitutionTableNumberCodec codec;
	
	public MongoChannelStore(DatabasedMongo mongo) {
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
        return Iterables.transform(collection.find(new BasicDBObject(ID,new BasicDBObject(IN, ids))), DB_TO_CHANNEL_TRANSLATOR);
    }

	@Override
	public Maybe<Channel> fromUri(String uri) {
		DBObject dbo = collection.findOne(where().fieldEquals(IdentifiedTranslator.CANONICAL_URL, uri).build());
		if(dbo == null) {
			return Maybe.nothing();
		}
		return Maybe.just(translator.fromDBObject(dbo, null));
	}
	
	@Override
	public Maybe<Channel> fromKey(String key) {
		DBObject dbo = collection.findOne(where().fieldEquals(ChannelTranslator.KEY, key).build());
		if(dbo == null) {
			return Maybe.nothing();
		}
		return Maybe.just(translator.fromDBObject(dbo, null));
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

        Iterable<Channel> aliasedChannels = Iterables.transform(collection.find(new BasicDBObject("aliases", prefixPattern)), DB_TO_CHANNEL_TRANSLATOR);

        Builder<String, Channel> channelMap = ImmutableMap.builder();
        for (Channel channel : aliasedChannels) {
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
