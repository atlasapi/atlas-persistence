package org.atlasapi.persistence.content.mongo;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;

import org.atlasapi.media.entity.ChildRef;
import org.atlasapi.media.entity.Person;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.atlasapi.persistence.media.entity.ContentTranslator;
import org.atlasapi.persistence.media.entity.CrewMemberTranslator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.persistence.mongo.MongoConstants;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;

public class ItemCrewRefUpdater {

    private static final String PERSON_URI_KEY = 
            ContentTranslator.PEOPLE+"."+CrewMemberTranslator.PERSON_URI;
    private static final String PERSON_ID_UPDATE_KEY = 
            ContentTranslator.PEOPLE+".$."+CrewMemberTranslator.PERSON_ID;

    private final MongoContentTables tables;
    private final LookupEntryStore entryStore;

    public ItemCrewRefUpdater(DatabasedMongo db, LookupEntryStore entryStore) {
        this.entryStore = entryStore;
        this.tables = new MongoContentTables(checkNotNull(db));
    }
    
    public void updateCrewRefInItems(Person person) {
        if (person.getCanonicalUri() == null || person.getId() == null) {
            return;
        }
        for (ChildRef content : person.getContents()) {
            setRefInContent(person.getCanonicalUri(), person.getId(), content);
        }
    }

    private void setRefInContent(String personUri, Long personId, ChildRef content) {
        DBCollection coll = collectionFor(content);
        if (coll == null) {
            return;
        }
        coll.update(
            new BasicDBObject(ImmutableMap.of(
                MongoConstants.ID, content.getUri(),
                PERSON_URI_KEY, personUri
            )),
            new BasicDBObject(
                MongoConstants.SET, 
                new BasicDBObject(PERSON_ID_UPDATE_KEY, personId)
            ),
            MongoConstants.NO_UPSERT,
            MongoConstants.SINGLE
        );
    }

    private DBCollection collectionFor(ChildRef content) {
        List<String> uris = ImmutableList.of(content.getUri());
        Iterable<LookupEntry> entries = entryStore.entriesForCanonicalUris(uris);
        LookupEntry entry = Iterables.getOnlyElement(entries, null);
        if (entry != null) {
            return tables.collectionFor(entry.lookupRef().category());
        }
        return null;
    }

}
