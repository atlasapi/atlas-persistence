package org.atlasapi.persistence.content.elasticsearch.support;

import java.util.Date;
import org.atlasapi.persistence.content.elasticsearch.schema.ESContent;
import org.atlasapi.persistence.content.elasticsearch.schema.ESLocation;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

public class AvailabilityQueryBuilder {

    public static QueryBuilder build(Date when, float boost) {
        return QueryBuilders.nestedQuery(ESContent.LOCATIONS,
                QueryBuilders.customBoostFactorQuery(
                QueryBuilders.boolQuery().
                must(QueryBuilders.rangeQuery(ESLocation.AVAILABILITY_TIME).gte(when)).
                must(QueryBuilders.rangeQuery(ESLocation.AVAILABILITY_END_TIME).lt(when))).
                boostFactor(boost));
    }
}
