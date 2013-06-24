package org.atlasapi.persistence.content.mongo;

import static com.metabroadcast.common.persistence.mongo.MongoBuilders.where;

import java.net.UnknownHostException;
import java.util.List;

import org.atlasapi.media.entity.ContentGroup;
import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.Person;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.PeopleListerListener;
import org.atlasapi.persistence.content.people.PersonStore;
import org.atlasapi.persistence.lookup.TransitiveLookupWriter;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.atlasapi.persistence.lookup.mongo.MongoLookupEntryStore;
import org.atlasapi.persistence.media.entity.IdentifiedTranslator;
import org.atlasapi.persistence.media.entity.PersonTranslator;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.persistence.mongo.MongoConstants;
import com.metabroadcast.common.persistence.mongo.MongoQueryBuilder;
import com.metabroadcast.common.persistence.mongo.MongoSortBuilder;
import com.metabroadcast.common.time.DateTimeZones;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

public class MongoPersonStore implements PersonStore {
    
    private static final Logger log = LoggerFactory.getLogger(MongoPersonStore.class);
    
    static final int MONGO_SCAN_BATCH_SIZE = 100;

	private static final MongoSortBuilder SORT_BY_ID = new MongoSortBuilder().ascending(MongoConstants.ID); 
	
    private final DBCollection collection;
    private final PersonTranslator translator = new PersonTranslator();

    private final TransitiveLookupWriter equivalenceWriter;
    private final LookupEntryStore lookupEntryStore;
    private final ItemCrewRefUpdater itemCrewRefUpdater;

    public MongoPersonStore(DatabasedMongo db, TransitiveLookupWriter equivalenceWriter, LookupEntryStore lookupEntryStore) {
        this.collection = db.collection("people");
        this.equivalenceWriter = equivalenceWriter;
        this.lookupEntryStore = lookupEntryStore;
        this.itemCrewRefUpdater = new ItemCrewRefUpdater(db, lookupEntryStore);
    }

    @Override
    public void updatePersonItems(Person person) {
        DBObject idQuery = translator.idQuery(person.getCanonicalUri()).build();
        
        collection.update(idQuery, translator.updateContentUris(person), true, false);
        itemCrewRefUpdater.updateCrewRefInItems(person);
    }

    @Override
    public Optional<Person> person(String uri) {
        List<Person> people = translator.fromDBObjects(translator.idQuery(uri).find(collection));
        return Optional.fromNullable(Iterables.getFirst(people, null));
    }
    
    @Override
    public Optional<Person> person(Long id) {
        DBObject q = where().fieldEquals(IdentifiedTranslator.OPAQUE_ID, id).build();
        DBObject personDbo = collection.findOne(q);
        if (personDbo == null) {
            return Optional.absent();
        }
        return Optional.fromNullable(translator.fromDBObject(personDbo, null));
    }

    @Override
    public Iterable<Person> people(Iterable<LookupRef> lookupRefs) {
        DBCursor found = collection.find(where().idIn(Iterables.transform(lookupRefs, LookupRef.TO_URI)).build());
        return translator.fromDBObjects(found);
    }
    
    @Override
    public void createOrUpdatePerson(Person person) {
        person.setLastUpdated(new DateTime(DateTimeZones.UTC));
        person.setMediaType(null);
        
        DBObject idQuery = translator.idQuery(person.getCanonicalUri()).build();
        collection.update(idQuery, translator.toDBObject(null, person), true, false);
        lookupEntryStore.store(LookupEntry.lookupEntryFrom(person));
        writeEquivalences(person);
    }
    
    @Override
	public void list(PeopleListerListener handler) {
    	String fromId = null;
		while (true) {
            List<Person> roots = ImmutableList.copyOf(batch(MONGO_SCAN_BATCH_SIZE, fromId));
            if (Iterables.isEmpty(roots)) {
                break;
            }
            for (Person person : roots) {
            	handler.personListed(person);
            }
            ContentGroup last = Iterables.getLast(roots);
            fromId = last.getCanonicalUri();
        }
	}
    
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

	private Iterable<Person> batch(int batchSize, String fromId) {
		MongoQueryBuilder query = where();
    	if (fromId != null) {
    		query.fieldGreaterThan(MongoConstants.ID, fromId);
    	}
        return Iterables.transform(query.find(collection, SORT_BY_ID, batchSize), TO_PERSON);
	}
	
	private final Function<DBObject, Person> TO_PERSON = new Function<DBObject, Person>() {
		@Override
		public Person apply(DBObject input) {
			return translator.fromDBObject(input, null);
		}
	};
	
	private void createEquivalences() {
	    DBCursor cursor = collection.find();
	    for(DBObject dbo : cursor) {
	        try {
	            Person person = translator.fromDBObject(dbo, null);
	            lookupEntryStore.store(LookupEntry.lookupEntryFrom(person));
	        }
	        catch (Exception e) {
	            log.error("Problem with a person", e);
	        }
	    }
	    
	}
	
	public static void main(String[] args) throws UnknownHostException {
	    Mongo mongo = new Mongo(System.getProperty("mongo.host", "127.0.0.1"));
        String dbName = System.getProperty("mongo.dbName", "atlas");
        DatabasedMongo dbMongo = new DatabasedMongo(mongo, dbName);
        LookupEntryStore entryStore = new MongoLookupEntryStore(dbMongo.collection("peopleLookup"));
	    MongoPersonStore store = new MongoPersonStore(dbMongo, TransitiveLookupWriter.explicitTransitiveLookupWriter(new MongoLookupEntryStore(dbMongo.collection("peopleLookup"))), entryStore);
	    store.createEquivalences();
	    
	}
}
