package org.uriplay.persistence.equiv;

import java.util.Set;

import org.uriplay.media.entity.Equiv;
import org.uriplay.persistence.media.entity.EquivTranslator;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

public class MongoEquivStore implements EquivalentUrlStore {

	// Prevent choking on really long chains of equivalence
	private static final int MAX_PATH = 5;
	
	private DBCollection equivCollection;
	private final EquivTranslator equivTranslator = new EquivTranslator();

	public MongoEquivStore(Mongo mongo, String dbName) {
		this.equivCollection = mongo.getDB(dbName).getCollection("equiv");
	}

	@Override
	public Set<String> get(Iterable<String> ids) {
	     Set<String> currentRoots = ImmutableSet.copyOf(ids);
	     Set<String> urls = Sets.newHashSet(currentRoots);
		 
	     for (int i = 0; i < MAX_PATH; i++) {
			 Set<String> seenThisIteration = getSinglePath(currentRoots);
			 currentRoots = ImmutableSet.copyOf(Sets.difference(seenThisIteration, urls));
			 urls.addAll(seenThisIteration);
		 
			 if (currentRoots.isEmpty()) {
				 break;
			 }
	     }
		 urls.removeAll(Sets.newHashSet(ids));
		 return urls;
	}
	
	private Set<String> getSinglePath(Set<String> ids) {
		 Set<String> urls = Sets.newHashSet();
		 urls.addAll(urlsFrom(equivTranslator.findPathLengthOneLeft(ids).find(equivCollection)));
		 urls.addAll(urlsFrom(equivTranslator.findPathLengthOneRight(ids).find(equivCollection)));
		 return urls;
	}

	@Override
	public void store(Equiv equiv) {
		equivCollection.insert(equivTranslator.toDBObject(equiv));
	}
	
	public Set<String> urlsFrom(Iterable<DBObject> dbObjects) {
		Set<String> urls = Sets.newHashSet();
		for(Equiv equiv : equivTranslator.fromDBObject(dbObjects)) {
			urls.add(equiv.left());
			urls.add(equiv.right());
		}
		return urls;
	}
}
