package org.atlasapi.persistence.output;

import static com.metabroadcast.common.persistence.mongo.MongoConstants.ID;
import static org.atlasapi.persistence.media.entity.IdentifiedTranslator.CURIE;
import static org.atlasapi.persistence.media.entity.IdentifiedTranslator.OPAQUE_ID;

import java.math.BigInteger;

import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.media.entity.simple.BrandSummary;
import org.atlasapi.media.entity.simple.SeriesSummary;
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

    @Override
    public Optional<BrandSummary> summarizeTopLevelContainer(ParentRef container) {
        DBObject containerDbo = containers.findOne(container.getUri(), fields);

        if (containerDbo == null) {
            return Optional.absent();
        }
        
        BrandSummary summary = new BrandSummary();
        
        Long id = TranslatorUtils.toLong(containerDbo, OPAQUE_ID);
        summary.setId(id != null ? idCodec.encode(BigInteger.valueOf(id)) : null);
        summary.setUri(TranslatorUtils.toString(containerDbo, ID));
        summary.setCurie(TranslatorUtils.toString(containerDbo, CURIE));
        summary.setTitle(TranslatorUtils.toString(containerDbo, title));
        summary.setDescription(TranslatorUtils.toString(containerDbo, description));
        summary.setType(TranslatorUtils.toString(containerDbo, type));
        return Optional.of(summary);
    }
    
    @Override
    public Optional<SeriesSummary> summarizeSeries(ParentRef series) {
        DBObject containerDbo = programmeGroups.findOne(series.getUri(), fields);

        if (containerDbo == null) {
            return Optional.absent();
        }
        
        SeriesSummary summary = new SeriesSummary();
        Long id = TranslatorUtils.toLong(containerDbo, OPAQUE_ID);
        summary.setId(id != null ? idCodec.encode(BigInteger.valueOf(id)) : null);
        summary.setUri(TranslatorUtils.toString(containerDbo, ID));
        summary.setCurie(TranslatorUtils.toString(containerDbo, CURIE));
        summary.setTitle(TranslatorUtils.toString(containerDbo, title));
        summary.setDescription(TranslatorUtils.toString(containerDbo, description));
        summary.setSeriesNumber(TranslatorUtils.toInteger(containerDbo, seriesNumber));
        summary.setTotalEpisodes(TranslatorUtils.toInteger(containerDbo, totalEpisodes));
        summary.setType(TranslatorUtils.toString(containerDbo, type));
        return Optional.of(summary);
    }

}