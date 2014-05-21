package org.atlasapi.persistence.service;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Service;

import com.google.common.base.Optional;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;


public class CachingServiceResolver implements ServiceResolver {

    private final ServiceResolver delegate;    
    private final LoadingCache<Long, Optional<Service>> cache = 
            CacheBuilder.newBuilder()
                        .expireAfterWrite(5, TimeUnit.MINUTES)
                        .build(new CacheLoader<Long, Optional<Service>>() {

                            @Override
                            public Optional<Service> load(Long key) throws Exception {
                                return delegate.serviceFor(key);
                            }
                            
                        });
    


    public CachingServiceResolver(ServiceResolver delegate) {
        this.delegate = checkNotNull(delegate);
    }
    
    @Override
    public Optional<Service> serviceFor(long id) {
        try {
            return cache.get(id);
        } catch (ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public Iterable<Service> servicesFor(Alias alias) {
        throw new UnsupportedOperationException();
    }
}
