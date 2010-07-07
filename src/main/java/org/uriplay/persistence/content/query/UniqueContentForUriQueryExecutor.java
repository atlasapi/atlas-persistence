package org.uriplay.persistence.content.query;

import java.util.List;
import java.util.Set;

import org.uriplay.content.criteria.AttributeQuery;
import org.uriplay.content.criteria.ContentQuery;
import org.uriplay.content.criteria.attribute.Attribute;
import org.uriplay.content.criteria.attribute.Attributes;
import org.uriplay.media.entity.Brand;
import org.uriplay.media.entity.Content;
import org.uriplay.media.entity.Countries;
import org.uriplay.media.entity.Country;
import org.uriplay.media.entity.Item;
import org.uriplay.media.entity.Playlist;
import org.uriplay.media.entity.Publisher;

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

        return isByUri(query, Attributes.BRAND_URI) ? removeDuplicateElements(results, query) : results;
    }

    @Override
    public List<Item> executeItemQuery(ContentQuery query) {
        List<Item> results = delegate.executeItemQuery(query);

        return isByUri(query, Attributes.ITEM_URI) ? removeDuplicateElements(results, query) : results;
    }

    @Override
    public List<Playlist> executePlaylistQuery(ContentQuery query) {
        List<Playlist> results = delegate.executePlaylistQuery(query);

        return isByUri(query, Attributes.PLAYLIST_URI) ? removeDuplicateElements(results, query) : results;
    }

    private boolean isByUri(ContentQuery query, Attribute<?> attribute) {
        Maybe<AttributeQuery<?>> byUri = QueryFragmentExtractor.extract(query, Sets.<Attribute<?>> newHashSet(attribute));
        return byUri.hasValue();
    }

    private <T extends Content> List<T> removeDuplicateElements(List<T> results, ContentQuery query) {
        List<T> nonDuplicates = Lists.newArrayList();

        for (T result : results) {
            Maybe<T> existing = existing(nonDuplicates, result);
            if (existing.isNothing()) {
                nonDuplicates.add(result);
            } else {
                if (shouldSwap(existing.requireValue(), result, query)) {
                    int index = nonDuplicates.indexOf(existing.requireValue());
                    if (index != -1) {
                        nonDuplicates.remove(existing.requireValue());
                        nonDuplicates.add(index, result);
                    }
                }
            }
        }

        return nonDuplicates;
    }
    
    @SuppressWarnings("unchecked")
    private boolean shouldSwap(Content existing, Content duplicate, ContentQuery query) {
        Maybe<AttributeQuery<?>> withLocation = QueryFragmentExtractor.extract(query, Sets.<Attribute<?>> newHashSet(Attributes.POLICY_AVAILABLE_COUNTRY));
        if (withLocation.hasValue()) {
            Set<Country> countries = Countries.fromCodes((List<String>) withLocation.requireValue().getValue());

            Maybe<Publisher> existingPublisher = Publisher.fromKey(existing.getPublisher());
            Maybe<Publisher> duplicatePublisher = Publisher.fromKey(duplicate.getPublisher());

            if (duplicatePublisher.hasValue() && countries.contains(duplicatePublisher.requireValue().country())
                    && (existingPublisher.isNothing() || !countries.contains(existingPublisher.requireValue().country()))) {
                return true;
            }
        }
        return false;
    }

    private <T extends Content> Maybe<T> existing(List<T> nonDuplicates, T element) {
        for (T nonDuplicate : nonDuplicates) {
            if (!Sets.intersection(element.getAllUris(), nonDuplicate.getAllUris()).isEmpty()) {
                return Maybe.fromPossibleNullValue(nonDuplicate);
            }
        }
        return Maybe.nothing();
    }
}
