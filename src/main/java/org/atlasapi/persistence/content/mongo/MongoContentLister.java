package org.atlasapi.persistence.content.mongo;

import java.util.List;

import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.ContentGroup;
import org.atlasapi.persistence.content.RetrospectiveContentLister;

import com.metabroadcast.common.persistence.mongo.DatabasedMongo;

public class MongoContentLister implements RetrospectiveContentLister {

    private final DatabasedMongo mongo;

    public MongoContentLister(DatabasedMongo mongo) {
        this.mongo = mongo;
    }

    @Override
    public List<Content> listAllRoots(String fromId, int batchSize) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<ContentGroup> listAllContentGroups(String fromId, int batchSize) {
        throw new UnsupportedOperationException();
    }

}
