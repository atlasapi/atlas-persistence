package org.atlasapi.persistence.content.organisation;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.metabroadcast.common.persistence.mongo.MongoBuilders.where;

import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.Organisation;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.audit.PersistenceAuditLog;
import org.atlasapi.persistence.content.mongo.ItemCrewRefUpdater;
import org.atlasapi.persistence.lookup.TransitiveLookupWriter;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.atlasapi.persistence.media.entity.IdentifiedTranslator;
import org.atlasapi.persistence.media.entity.OrganisationTranslator;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.persistence.mongo.MongoConstants;
import com.metabroadcast.common.persistence.mongo.MongoSortBuilder;
import com.metabroadcast.common.time.DateTimeZones;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;


public class MongoOrganisationStore implements OrganisationStore {

    private static final Logger log = LoggerFactory.getLogger(MongoOrganisationStore.class);
    
    static final int MONGO_SCAN_BATCH_SIZE = 100;

    private static final MongoSortBuilder SORT_BY_ID = new MongoSortBuilder().ascending(MongoConstants.ID); 
    
    private final DBCollection collection;
    private final OrganisationTranslator translator = new OrganisationTranslator();

    private final TransitiveLookupWriter equivalenceWriter;
    private final LookupEntryStore lookupEntryStore;
    private final ItemCrewRefUpdater itemCrewRefUpdater;
    private final PersistenceAuditLog persistenceAuditLog;

    public MongoOrganisationStore(DatabasedMongo db, TransitiveLookupWriter equivalenceWriter, 
            LookupEntryStore lookupEntryStore, PersistenceAuditLog persistenceAuditLog) {
        this.collection = checkNotNull(db).collection("organisations");
        this.equivalenceWriter = checkNotNull(equivalenceWriter);
        this.lookupEntryStore = checkNotNull(lookupEntryStore);
        this.itemCrewRefUpdater = new ItemCrewRefUpdater(db, lookupEntryStore);
        this.persistenceAuditLog = checkNotNull(persistenceAuditLog);
    }
    
    @Override
    public void updateOrganisationItems(Organisation organisation) {
        DBObject idQuery = where().idEquals(organisation.getCanonicalUri()).build();
        
        collection.update(idQuery, translator.updateContentUris(organisation), true, false);
        itemCrewRefUpdater.updateCrewRefInItems(organisation);
    }

    @Override
    public void createOrUpdateOrganisation(Organisation organisation) {
        organisation.setLastUpdated(new DateTime(DateTimeZones.UTC));
        organisation.setMediaType(null);
        
        DBObject idQuery = where().idEquals(organisation.getCanonicalUri()).build();
        persistenceAuditLog.logWrite(organisation);
        collection.update(idQuery, translator.toDBObject(organisation), true, false);
        lookupEntryStore.store(LookupEntry.lookupEntryFrom(organisation));
        writeEquivalences(organisation);
    }
    
    // TODO extract this to somewhere common between Person/Organisation... DescribedTranslator?
    private void writeEquivalences(Described content) {
        if (!content.getEquivalentTo().isEmpty()) {
            ImmutableSet<Publisher> publishers = publishers(content);
            Iterable<String> equivalentUris = Iterables.transform(content.getEquivalentTo(), LookupRef.TO_URI);
            equivalenceWriter.writeLookup(content.getCanonicalUri(), equivalentUris, publishers);
        }
    }

    private ImmutableSet<Publisher> publishers(Described content) {
        return ImmutableSet.<Publisher>builder().add(content.getPublisher()).addAll(Iterables.transform(content.getEquivalentTo(), LookupRef.TO_SOURCE)).build();
    }

    @Override
    public Optional<Organisation> organisation(String uri) {
        return Optional.fromNullable(translator.fromDBObject(collection.findOne(where().idEquals(uri).build())));
    }

    @Override
    public Optional<Organisation> organisation(Long id) {
        DBObject query = where().fieldEquals(IdentifiedTranslator.OPAQUE_ID, id).build();
        return Optional.fromNullable(translator.fromDBObject(collection.findOne(query)));
    }
    
    // have common store between personstore and organisationstore?
    @Override
    public Iterable<Organisation> organisations(Iterable<LookupRef> lookupRefs) {
        DBCursor found = collection.find(where().idIn(Iterables.transform(lookupRefs, LookupRef.TO_URI)).build());
        return translator.fromDBObjects(found);
    }

}
