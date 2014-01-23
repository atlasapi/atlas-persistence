package org.atlasapi.persistence.media.entity;

import java.util.Set;

import org.atlasapi.media.entity.EntityType;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.SimilarContentRef;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;


public class SimilarContentRefTranslator {

    private static final String URI_KEY = "uri";
    private static final String ID_KEY = "id";
    private static final String ENTITY_TYPE_KEY = "type";
    private static final String PUBLISHERS_WITH_AVAILABLE_CONTENT_KEY = "publishersWithAvailableContent";
    private static final String PUBLISHERS_WITH_UPCOMING_CONTENT_KEY = "publishersWithUpcomingContent";
    
    public Set<SimilarContentRef> fromDBObjects(Iterable<DBObject> dbos) {
        return ImmutableSet.copyOf(Iterables.transform(dbos, TO_SIMILAR_CONTENT_REF));
    }
    
    public BasicDBList toDBList(Iterable<SimilarContentRef> childRefs) {
        BasicDBList list = new BasicDBList();
        list.addAll(ImmutableList.copyOf(Iterables.transform(childRefs, TO_DBO)));
        return list;
    }
    
    private static Function<SimilarContentRef, DBObject> TO_DBO = 
        new Function<SimilarContentRef, DBObject>() {

            @Override
            public DBObject apply(SimilarContentRef similar) {
                DBObject dbo = new BasicDBObject();
                TranslatorUtils.from(dbo, URI_KEY, similar.getUri());
                TranslatorUtils.from(dbo, ID_KEY, similar.getId());
                TranslatorUtils.from(dbo, ENTITY_TYPE_KEY, similar.getEntityType().toString());
                TranslatorUtils.fromList(dbo, Ordering.natural().sortedCopy(
                        Iterables.transform(similar.getPublishersWithAvailableContent(), Publisher.TO_KEY)), 
                        PUBLISHERS_WITH_AVAILABLE_CONTENT_KEY);
                TranslatorUtils.fromList(dbo, Ordering.natural().sortedCopy(
                        Iterables.transform(similar.getPublishersWithUpcomingContent(), Publisher.TO_KEY)), 
                        PUBLISHERS_WITH_UPCOMING_CONTENT_KEY);
                return dbo;
            }
        
    };
    
    private static Function<DBObject, SimilarContentRef> TO_SIMILAR_CONTENT_REF = 
        new Function<DBObject, SimilarContentRef>() {

            @Override
            public SimilarContentRef apply(DBObject dbo) {
                TranslatorUtils.toString(dbo, URI_KEY);
                EntityType type = EntityType.from((String) dbo.get(ENTITY_TYPE_KEY));
                Long id = (Long) dbo.get(ID_KEY);
                int score = 0;
               
                return SimilarContentRef.builder()
                                        .withId(id)
                                        .withEntityType(type)
                                        .withScore(score)
                                        .withPublishersWithAvailableContent(publishersFrom(dbo, 
                                                            PUBLISHERS_WITH_AVAILABLE_CONTENT_KEY))
                                        .withPublishersWithUpcomingContent(publishersFrom(dbo, 
                                                            PUBLISHERS_WITH_UPCOMING_CONTENT_KEY))
                                        .build();

            }
        
    };
    
    private static Set<Publisher> publishersFrom(DBObject dbo, String key) {
        return ImmutableSet.copyOf(Iterables.transform(TranslatorUtils.toList(dbo, key), Publisher.FROM_KEY));
    }
    
    // todo: order the publishers on write
}
