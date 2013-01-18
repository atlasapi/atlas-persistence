package org.atlasapi.persistence.content.mongo;

import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.content.ResolvedContent.ResolvedContentBuilder;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

import org.atlasapi.media.common.Id;
import org.atlasapi.media.entity.ContentGroup;
import org.atlasapi.persistence.content.ContentCategory;
import org.atlasapi.persistence.content.ContentGroupResolver;
import org.atlasapi.persistence.media.entity.ContentGroupTranslator;
import org.atlasapi.persistence.media.entity.IdentifiedTranslator;
import static com.metabroadcast.common.persistence.mongo.MongoBuilders.where;
import com.mongodb.DBCursor;
import java.util.HashSet;
import java.util.Set;

public class MongoContentGroupResolver implements ContentGroupResolver {

    private final ContentGroupTranslator contentGroupTranslator;
    private final DBCollection contentGroups;

    public MongoContentGroupResolver(DatabasedMongo mongo) {
        MongoContentTables contentTables = new MongoContentTables(mongo);
        this.contentGroups = contentTables.collectionFor(ContentCategory.CONTENT_GROUP);
        this.contentGroupTranslator = new ContentGroupTranslator();
    }

    @Override
    public ResolvedContent findByCanonicalUris(Iterable<String> uris) {
        ResolvedContentBuilder results = ResolvedContent.builder();

        for (String uri : uris) {
            DBObject found = contentGroups.findOne(where().fieldEquals(IdentifiedTranslator.CANONICAL_URL, uri).build());
            if (found != null) {
                ContentGroup contentGroup = contentGroupTranslator.fromDBObject(found, new ContentGroup());
                //results.put(uri, contentGroup);
            }
        }

        return results.build();
    }

    @Override
    public ResolvedContent findByIds(Iterable<Id> ids) {
        ResolvedContentBuilder results = ResolvedContent.builder();

        for (Id id : ids) {
            DBObject found = contentGroups.findOne(where().fieldEquals(IdentifiedTranslator.ID, id.longValue()).build());
            if (found != null) {
                ContentGroup contentGroup = contentGroupTranslator.fromDBObject(found, new ContentGroup());
//                results.put(id, contentGroup);
            }
        }

        return results.build();
    }

    @Override
    public Iterable<ContentGroup> findAll() {
        Set<ContentGroup> results = new HashSet<ContentGroup>();

        DBCursor cursor = contentGroups.find();
        for (DBObject current : cursor) {
            ContentGroup contentGroup = contentGroupTranslator.fromDBObject(current, new ContentGroup());
            results.add(contentGroup);
        }

        return results;
    }
}
