package org.atlasapi.persistence.content.elasticsearch.support;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Specialization;
import org.atlasapi.persistence.content.elasticsearch.schema.EsContent;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.TermsFilterBuilder;

public class FiltersBuilder {

    public static TermsFilterBuilder buildForPublishers(Iterable<Publisher> publishers) {
        return FilterBuilders.termsFilter(EsContent.PUBLISHER, Iterables.transform(publishers, Publisher.TO_KEY));
    }

    public static TermsFilterBuilder buildForSpecializations(Iterable<Specialization> specializations) {
        return FilterBuilders.termsFilter(EsContent.SPECIALIZATION, Iterables.transform(specializations, new Function<Specialization, String>() {

            @Override
            public String apply(Specialization input) {
                return input.name();
            }
        }));
    }
}
