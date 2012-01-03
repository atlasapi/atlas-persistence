package org.atlasapi.persistence.channels;

import static com.metabroadcast.common.persistence.mongo.MongoBuilders.where;

import java.util.Map;

import org.atlasapi.media.entity.Channel;
import org.atlasapi.persistence.ids.MongoSequentialIdGenerator;
import org.atlasapi.persistence.media.entity.ChannelTranslator;
import org.atlasapi.persistence.media.entity.DescriptionTranslator;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.persistence.mongo.MongoConstants;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
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
		DBObject dbo = collection.findOne(where().fieldEquals(MongoConstants.ID, id).build());
		if(dbo == null) {
			return Maybe.nothing();
		}
		return Maybe.just(translator.fromDBObject(dbo, null));
	}

	@Override
	public Iterable<Channel> all() {
		DBCursor it = collection.find();
		return Iterables.transform(it, DB_TO_CHANNEL_TRANSLATOR);
	}

	@Override
	public Maybe<Channel> fromUri(String uri) {
		DBObject dbo = collection.findOne(where().fieldEquals(DescriptionTranslator.CANONICAL_URL, uri).build());
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
		
		Builder<String, Channel> channelMap = ImmutableMap.builder();
		for(Channel channel : Iterables.filter(all(), hasAliasPrefixedWith(aliasPrefix))) {
			for(String alias : channel.getAliases()) {
				if(alias.startsWith(aliasPrefix)) {
					channelMap.put(alias, channel);
				}
			}
		}
		return channelMap.build();
	}
	
	private Predicate<Channel> hasAliasPrefixedWith(final String aliasPrefix) {
		return new Predicate<Channel>() {
			
			@Override
			public boolean apply(Channel input) {
				for(String alias : input.getAliases()) {
					if(alias.startsWith(aliasPrefix)) {
						return true;
					}
				}
				return false;
			}	
		};
	}
	
	private Function<DBObject, Channel> DB_TO_CHANNEL_TRANSLATOR = new Function<DBObject, Channel>() {

		@Override
		public Channel apply(DBObject input) {
			return translator.fromDBObject(input, null);
		}
	};

}
