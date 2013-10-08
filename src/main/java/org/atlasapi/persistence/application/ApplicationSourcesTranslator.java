package org.atlasapi.persistence.application;

import java.util.List;
import java.util.Map;

import org.atlasapi.application.ApplicationSources;
import org.atlasapi.application.SourceReadEntry;
import org.atlasapi.application.SourceStatus;
import org.atlasapi.application.SourceStatus.SourceState;
import org.atlasapi.media.entity.Publisher;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class ApplicationSourcesTranslator {

    public static final String STATE_KEY = "state";
    public static final String PUBLISHER_KEY = "publisher";
    public static final String SOURCES_KEY = "sources";
    public static final String PRECEDENCE_KEY = "precedence";
    public static final String WRITABLE_KEY = "writable";

    public DBObject toDBObject(ApplicationSources configuration) {
        BasicDBObject dbo = new BasicDBObject();

        TranslatorUtils.from(dbo, SOURCES_KEY, sourceStatusesToList(configuration.getReads()));

        if (configuration.isPrecedenceEnabled()) {
            List<String> precedenceOrder = Lists.newLinkedList();
            for (SourceReadEntry source : configuration.getReads()) {
                precedenceOrder.add(source.getPublisher().key());
            }
            TranslatorUtils.fromList(dbo, precedenceOrder, PRECEDENCE_KEY);
        } else {
            dbo.put(PRECEDENCE_KEY, null);
        }

        dbo.put(WRITABLE_KEY, Iterables.transform(configuration.getWrites(), Publisher.TO_KEY));

        return dbo;
    }

    private BasicDBList sourceStatusesToList(List<SourceReadEntry> sourceStatuses) {
        BasicDBList statuses = new BasicDBList();
        for (SourceReadEntry sourceStatus : sourceStatuses) {
            statuses.add(new BasicDBObject(ImmutableMap.of(
                    PUBLISHER_KEY, sourceStatus.getPublisher().key(),
                    STATE_KEY, sourceStatus.getSourceStatus().getState().toString().toLowerCase(),
                    "enabled", sourceStatus.getSourceStatus().isEnabled()
                    )));
        }
        return statuses;
    }

    public ApplicationSources fromDBObject(DBObject dbo) {

        List<Publisher> precedence = sourcePrecedenceFrom(dbo);
        List<DBObject> statusDbos = TranslatorUtils.toDBObjectList(dbo, SOURCES_KEY);
        List<SourceReadEntry> sourceStatuses = sourceStatusesFrom(statusDbos, precedence);

        List<String> writableKeys = TranslatorUtils.toList(dbo, WRITABLE_KEY);
        List<Publisher> writableSources = Lists.transform(writableKeys, Publisher.FROM_KEY);

        boolean precedenceFlag = precedence != null && !precedence.isEmpty();
        return ApplicationSources.builder()
                .withPrecedence(precedenceFlag)
                .withReads(sourceStatuses)
                .withWrites(writableSources)
                .build();
    }

    private List<Publisher> sourcePrecedenceFrom(DBObject dbo) {
        if (dbo.get(PRECEDENCE_KEY) == null) {
            return null;
        }
        List<String> sourceKeys = TranslatorUtils.toList(dbo, PRECEDENCE_KEY);
        return Lists.transform(sourceKeys, Publisher.FROM_KEY);
    }

    private List<SourceReadEntry> sourceStatusesFrom(List<DBObject> list,
            List<Publisher> precedence) {
        Map<Publisher, SourceStatus> readsBuilder = Maps.newHashMap();
        for (DBObject dbo : list) {
            readsBuilder.put(
                Publisher.fromPossibleKey(TranslatorUtils.toString(dbo, PUBLISHER_KEY)).get(),
                sourceStatusFrom(dbo)
            );
        }
        Map<Publisher, SourceStatus> reads = ImmutableMap.copyOf(readsBuilder);
        if (precedence != null && !precedence.isEmpty()) {
            // reorder if precedence enabled
            return asOrderedList(reads, precedence);
        } else {
            return asOrderedList(reads, reads.keySet());
        }
    }
    
    private List<SourceReadEntry> asOrderedList(Map<Publisher, SourceStatus> readsMap, Iterable<Publisher> order) {
        ImmutableList.Builder<SourceReadEntry> builder = ImmutableList.builder();
        for (Publisher source : order) {
            builder.add(new SourceReadEntry(
                      source,
                      readsMap.get(source)
                    )
            );
        }   
        return builder.build();
    }

    private SourceStatus sourceStatusFrom(DBObject dbo) {
        if (TranslatorUtils.toBoolean(dbo, "enabled")) {
            return SourceStatus.AVAILABLE_ENABLED;
        }
        switch (SourceState.valueOf(TranslatorUtils.toString(dbo, STATE_KEY).toUpperCase())) {
        case AVAILABLE:
            return SourceStatus.AVAILABLE_DISABLED;
        case REQUESTED:
            return SourceStatus.REQUESTED;
        case REVOKED:
            return SourceStatus.REVOKED;
        default:
            return SourceStatus.UNAVAILABLE;
        }
    }

}
