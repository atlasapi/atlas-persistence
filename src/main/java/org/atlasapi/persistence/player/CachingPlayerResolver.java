package org.atlasapi.persistence.player;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Player;

import com.google.common.base.Optional;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;


public class CachingPlayerResolver implements PlayerResolver {

    private final PlayerResolver delegate;    
    private final LoadingCache<Long, Optional<Player>> cache = 
            CacheBuilder.newBuilder()
                        .expireAfterWrite(5, TimeUnit.MINUTES)
                        .build(new CacheLoader<Long, Optional<Player>>() {

                            @Override
                            public Optional<Player> load(Long key) throws Exception {
                                return delegate.playerFor(key);
                            }
                            
                        });
    


    public CachingPlayerResolver(PlayerResolver delegate) {
        this.delegate = checkNotNull(delegate);
    }
    
    @Override
    public Optional<Player> playerFor(long id) {
        try {
            return cache.get(id);
        } catch (ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public Iterable<Player> playersFor(Alias alias) {
        throw new UnsupportedOperationException();
    }
}
