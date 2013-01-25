package org.atlasapi.persistence.content.mongo;

import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.content.ResolvedContent.ResolvedContentBuilder;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import org.atlasapi.media.entity.ContentGroup;
import org.atlasapi.persistence.content.ContentCategory;
import org.atlasapi.persistence.content.ContentGroupResolver;
import org.atlasapi.persistence.media.entity.ContentGroupTranslator;
import org.atlasapi.persistence.media.entity.IdentifiedTranslator;
import static com.metabroadcast.common.persistence.mongo.MongoBuilders.where;
import com.mongodb.DBCursor;
import java.util.HashSet;
import java.util.Set;

import javax.annotation.Nullable;

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
                results.put(uri, contentGroup);
            }
        }

        return results.build();
    }

    @Override
    public ResolvedContent findByIds(Iterable<Long> ids) {
        ResolvedContentBuilder results = ResolvedContent.builder();

        for (Long id : ids) {
            DBObject found = contentGroups.findOne(where().fieldEquals(IdentifiedTranslator.ID, id).build());
            if (found != null) {
                ContentGroup contentGroup = TO_CONTENT_GROUP.apply(found);
                results.put(id.toString(), contentGroup);
            }
        }

        return results.build();
    }

    @Override
    public Iterable<ContentGroup> findAll() {
        return Iterables.transform(contentGroups.find(), TO_CONTENT_GROUP);
    }
    
    private final Function<DBObject, ContentGroup> TO_CONTENT_GROUP = new Function<DBObject, ContentGroup>() {

        @Override
        public ContentGroup apply(DBObject input) {
            return contentGroupTranslator.fromDBObject(input, null);
        }
        
    };
}
