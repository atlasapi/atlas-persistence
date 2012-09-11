package org.atlasapi.persistence.content.elasticsearch.support;

import java.util.List;
import java.util.Map;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.atlasapi.persistence.content.elasticsearch.schema.ESContent;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.FuzzyQueryBuilder;
import org.elasticsearch.index.query.PrefixQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;

public class TitleQueryBuilder {

    private static final Joiner JOINER = Joiner.on("");
    private static final int USE_PREFIX_SEARCH_UP_TO = 2;
    private static final Map<String, String> EXPANSIONS = ImmutableMap.<String, String>builder().put("dr", "doctor").put("rd", "road").build();

    public static QueryBuilder build(String title, float boost) {
        List<String> tokens = Strings.tokenize(title, true);
        BoolQueryBuilder query = null;
        if (shouldUsePrefixSearch(tokens)) {
            query = prefixSearch(Iterables.getOnlyElement(tokens));
        } else {
            query = fuzzyTermSearch(Strings.flatten(title), tokens);
        }
        return QueryBuilders.customBoostFactorQuery(query).boostFactor(boost);
    }

    private static boolean shouldUsePrefixSearch(List<String> tokens) {
        return tokens.size() == 1 && Iterables.getOnlyElement(tokens).length() <= USE_PREFIX_SEARCH_UP_TO;
    }

    private static BoolQueryBuilder prefixSearch(String token) {
        BoolQueryBuilder withExpansions = new BoolQueryBuilder();
        withExpansions.minimumNumberShouldMatch(1);
        withExpansions.should(prefixQuery(token));
        //
        String expanded = EXPANSIONS.get(token);
        if (expanded != null) {
            withExpansions.should(prefixQuery(expanded));
        }
        return withExpansions;
    }

    private static PrefixQueryBuilder prefixQuery(String prefix) {
        return new PrefixQueryBuilder(ESContent.FLATTENED_TITLE, prefix);
    }

    private static BoolQueryBuilder fuzzyTermSearch(String value, List<String> tokens) {
        BoolQueryBuilder queryForTerms = new BoolQueryBuilder();
        for (String token : tokens) {
            BoolQueryBuilder queryForThisTerm = new BoolQueryBuilder();
            queryForThisTerm.minimumNumberShouldMatch(1);

            PrefixQueryBuilder prefix = new PrefixQueryBuilder(ESContent.TITLE, token);
            prefix.boost(20);
            queryForThisTerm.should(prefix);

            FuzzyQueryBuilder fuzzy = new FuzzyQueryBuilder(ESContent.TITLE, token).minSimilarity(0.65f).prefixLength(USE_PREFIX_SEARCH_UP_TO);
            queryForThisTerm.should(fuzzy);

            queryForTerms.must(queryForThisTerm);
        }

        BoolQueryBuilder either = new BoolQueryBuilder();
        either.minimumNumberShouldMatch(1);
        either.should(queryForTerms);
        either.should(fuzzyWithoutSpaces(value));

        BoolQueryBuilder prefix = prefixSearch(value);
        prefix.boost(50);

        either.should(prefix);
        either.should(exactMatch(value, tokens));

        return either;
    }

    private static BoolQueryBuilder exactMatch(String value, Iterable<String> tokens) {
        BoolQueryBuilder exactMatch = new BoolQueryBuilder();
        exactMatch.minimumNumberShouldMatch(1);
        exactMatch.should(new TermQueryBuilder(ESContent.FLATTENED_TITLE, value));

        Iterable<String> transformed = Iterables.transform(tokens, new Function<String, String>() {

            @Override
            public String apply(String token) {
                String expanded = EXPANSIONS.get(token);
                if (expanded != null) {
                    return expanded;
                }
                return token;
            }
        });

        String flattenedAndExpanded = JOINER.join(transformed);

        if (!flattenedAndExpanded.equals(value)) {
            exactMatch.should(new TermQueryBuilder(ESContent.FLATTENED_TITLE, flattenedAndExpanded));
        }
        exactMatch.boost(100);
        return exactMatch;
    }

    private static FuzzyQueryBuilder fuzzyWithoutSpaces(String value) {
        return new FuzzyQueryBuilder(ESContent.FLATTENED_TITLE, value).minSimilarity(0.8f).prefixLength(USE_PREFIX_SEARCH_UP_TO);
    }
}
