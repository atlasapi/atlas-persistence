package org.atlasapi.persistence.output;

import static com.metabroadcast.common.persistence.mongo.MongoConstants.ID;
import static org.atlasapi.persistence.media.entity.IdentifiedTranslator.CURIE;
import static org.atlasapi.persistence.media.entity.IdentifiedTranslator.OPAQUE_ID;

import java.math.BigInteger;

import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.persistence.content.ContentCategory;

import com.google.common.base.Optional;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.persistence.mongo.MongoBuilders;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class MongoContainerSummaryResolver implements ContainerSummaryResolver {

    private final String title = "title";
    private final String description = "description";
    private final String seriesNumber = "seriesNumber";
    private final String totalEpisodes = "totalEpisodes";
    private final String type = "type";
    private final DBObject fields = MongoBuilders.select().fields(ID, CURIE, OPAQUE_ID, title).fields(seriesNumber, description, totalEpisodes, type).build();

    private final DBCollection containers;
    private final DBCollection programmeGroups;
    private final NumberToShortStringCodec idCodec;

    public MongoContainerSummaryResolver(DatabasedMongo mongo, NumberToShortStringCodec idCodec) {
        this.idCodec = idCodec;
        this.containers = mongo.collection(ContentCategory.CONTAINER.tableName());
        this.programmeGroups = mongo.collection(ContentCategory.PROGRAMME_GROUP.tableName());
    }


}
