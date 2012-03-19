package org.atlasapi.persistence.content.mongo;

import org.atlasapi.persistence.content.ContentCategory;
import org.atlasapi.persistence.media.entity.IdentifiedTranslator;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.time.Clock;
import com.mongodb.DBCollection;
import org.atlasapi.media.entity.ContentGroup;
import org.atlasapi.persistence.content.ContentGroupWriter;
import org.atlasapi.persistence.ids.MongoSequentialIdGenerator;
import org.atlasapi.persistence.media.entity.ContentGroupTranslator;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.metabroadcast.common.persistence.mongo.MongoBuilders.where;

public class MongoContentGroupWriter implements ContentGroupWriter {

    private final Clock clock;
    private final ContentGroupTranslator contentGroupTranslator;
    private final MongoSequentialIdGenerator idGenerator;
    private final DBCollection contentGroups;

    public MongoContentGroupWriter(DatabasedMongo mongo, Clock clock) {
        MongoContentTables contentTables = new MongoContentTables(mongo);
        this.clock = clock;
        this.contentGroups = contentTables.collectionFor(ContentCategory.CONTENT_GROUP);
        this.contentGroupTranslator = new ContentGroupTranslator();
        this.idGenerator = new MongoSequentialIdGenerator(mongo, ContentCategory.CONTENT_GROUP.tableName());
    }

    @Override
    public void createOrUpdate(ContentGroup contentGroup) {
        checkNotNull(contentGroup, "Tried to persist null content group");

        contentGroup.setLastFetched(clock.now());

        if (!contentGroup.hashChanged(contentGroupTranslator.hashCodeOf(contentGroup))) {
            return;
        } else {
            ensureId(contentGroup);
            contentGroup.setThisOrChildLastUpdated(clock.now());
            contentGroups.save(contentGroupTranslator.toDBObject(null, contentGroup));
        }
    }
    
    private void ensureId(ContentGroup contentGroup) {
        boolean noId = contentGroup.getId() == null;
        boolean noStoredId = contentGroup.getId() != null && contentGroups.count(where().fieldEquals(IdentifiedTranslator.ID, contentGroup.getId()).build()) == 0;
        if (noId || noStoredId) {
            contentGroup.setId(idGenerator.generateRaw());
        }
    }
}
