package org.atlasapi.persistence.content.mongo;

import static com.metabroadcast.common.persistence.mongo.MongoBuilders.where;

import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Player;
import org.atlasapi.persistence.media.entity.DescribedTranslator;
import org.atlasapi.persistence.media.entity.IdentifiedTranslator;
import org.atlasapi.persistence.media.entity.ImageTranslator;
import org.atlasapi.persistence.player.PlayerResolver;

import com.google.common.base.Optional;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;


public class MongoPlayerStore implements PlayerResolver {

    private static final String COLLECTION = "players";
    
    private final DBCollection collection;

    private DescribedTranslator playerTranslator;

    public MongoPlayerStore(DatabasedMongo mongo) {
        this.collection = mongo.collection(COLLECTION);
        this.playerTranslator = new DescribedTranslator(new IdentifiedTranslator(true), new ImageTranslator());
    }
    @Override
    public Optional<Player> playerFor(long id) {
        DBObject dbo = collection.findOne(where().idEquals(id).build());
        if (dbo == null) {
            return Optional.absent();
        }
        return Optional.of((Player) playerTranslator.fromDBObject(dbo, new Player()));
    }

    @Override
    public Iterable<Player> playersFor(Alias alias) {
        throw new UnsupportedOperationException();
    }
    
}
