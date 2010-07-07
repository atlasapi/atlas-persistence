package org.uriplay.persistence.content.query;

import java.util.List;

import org.uriplay.content.criteria.AttributeQuery;
import org.uriplay.content.criteria.ContentQuery;
import org.uriplay.content.criteria.attribute.Attribute;
import org.uriplay.content.criteria.attribute.Attributes;
import org.uriplay.media.entity.Brand;
import org.uriplay.media.entity.Content;
import org.uriplay.media.entity.Item;
import org.uriplay.media.entity.Playlist;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.metabroadcast.common.base.Maybe;

public class UniqueContentForUriQueryExecutor implements KnownTypeQueryExecutor {
    
    private final KnownTypeQueryExecutor delegate;

    public UniqueContentForUriQueryExecutor(KnownTypeQueryExecutor delegate) {
        this.delegate = delegate;
    }

    @Override
    public List<Brand> executeBrandQuery(ContentQuery query) {
        List<Brand> results = delegate.executeBrandQuery(query);
        
        return isByUri(query, Attributes.BRAND_URI) ? removeDuplicateElements(results) : results;
    }

    @Override
    public List<Item> executeItemQuery(ContentQuery query) {
        List<Item> results = delegate.executeItemQuery(query);
        
        return isByUri(query, Attributes.ITEM_URI) ? removeDuplicateElements(results) : results;
    }

    @Override
    public List<Playlist> executePlaylistQuery(ContentQuery query) {
        List<Playlist> results = delegate.executePlaylistQuery(query);
        
        return isByUri(query, Attributes.PLAYLIST_URI) ? removeDuplicateElements(results) : results;
    }
    
    private boolean isByUri(ContentQuery query, Attribute<?> attribute) {
        Maybe<AttributeQuery<?>> byUri = QueryFragmentExtractor.extract(query, Sets.<Attribute<?>>newHashSet(attribute));
        return byUri.hasValue();
    }

    private <T extends Content> List<T> removeDuplicateElements(List<T> results) {
        List<T> nonDuplicates = Lists.newArrayList();
        
        for (T result: results) {
            if (! alreadyExists(nonDuplicates, result)) {
                nonDuplicates.add(result);
            }
        }
        
        return nonDuplicates;
    }
    
    private <T extends Content> boolean alreadyExists(List<T> nonDuplicates, T element) {
        for (T nonDuplicate: nonDuplicates) {
            if (! Sets.intersection(element.getAllUris(), nonDuplicate.getAllUris()).isEmpty()) {
                return true;
            }
        }
        return false;
    }
}
