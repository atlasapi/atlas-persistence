package org.atlasapi.persistence.media.segment;

import static com.metabroadcast.common.persistence.mongo.MongoBuilders.where;
import static com.metabroadcast.common.persistence.mongo.MongoConstants.ID;
import static com.metabroadcast.common.persistence.mongo.MongoConstants.IN;
import static org.atlasapi.persistence.media.segment.SegmentTranslator.PUBLISHER_KEY;
import static org.atlasapi.persistence.media.segment.SegmentTranslator.SOURCE_ID_KEY;

import java.util.Map;

import org.atlasapi.media.entity.Publisher;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import org.atlasapi.media.segment.Segment;
import org.atlasapi.media.segment.SegmentRef;

public class MongoSegmentResolver implements SegmentResolver {

    private DBCollection segments;
    private SegmentTranslator translator;
    private final NumberToShortStringCodec idCodec;

    public MongoSegmentResolver(DatabasedMongo mongo, NumberToShortStringCodec idCodec) {
        this.segments = mongo.collection("segments");
        this.idCodec = idCodec;
        this.translator = new SegmentTranslator(idCodec);
    }
    
    @Override
    public Map<SegmentRef, Maybe<Segment>> resolveById(Iterable<SegmentRef> identifiers) {
        Map<SegmentRef, Segment> resolved = Maps.uniqueIndex(Iterables.transform(segmentsForIdentifiers(identifiers), new Function<DBObject, Segment>() {
            @Override
            public Segment apply(DBObject input) {
                return translator.fromDBObject(input, null);
            }
        }), Segment.TO_REF);
        
        ImmutableMap.Builder<SegmentRef, Maybe<Segment>> result = ImmutableMap.builder();
        for (SegmentRef segmentRef : identifiers) {
            result.put(segmentRef, Maybe.fromPossibleNullValue(resolved.get(segmentRef)));
        }
        return result.build();
    }

    private DBCursor segmentsForIdentifiers(Iterable<SegmentRef> identifiers) {
        return segments.find(new BasicDBObject(ID, new BasicDBObject(IN,decode(identifiers))));
    }

    private BasicDBList decode(Iterable<SegmentRef> identifiers) {
        BasicDBList list = new BasicDBList();
        list.addAll(ImmutableList.copyOf(Iterables.transform(identifiers, Functions.compose(new Function<String, Long>() {
            @Override
            public Long apply(String input) {
                return idCodec.decode(input).longValue();
            }
        }, SegmentRef.TO_ID))));
        return list;
    }

    @Override
    public Maybe<Segment> resolveForSource(Publisher source, String sourceId) {
        DBObject dbo = segments.findOne(where().fieldEquals(PUBLISHER_KEY, source.key()).fieldEquals(SOURCE_ID_KEY, sourceId).build());
        if (dbo == null) {
            return Maybe.nothing();
        }
        return Maybe.just(translator.fromDBObject(dbo, null));
    }

}
