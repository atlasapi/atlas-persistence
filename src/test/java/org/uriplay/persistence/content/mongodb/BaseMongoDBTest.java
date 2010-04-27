package org.uriplay.persistence.content.mongodb;

import java.io.File;
import java.io.IOException;

import org.jmock.integration.junit3.MockObjectTestCase;

import com.mongodb.Mongo;

public abstract class BaseMongoDBTest extends MockObjectTestCase {

	private static final String MONGODB_TEST_DB = "/tmp/mongodb/test/db";
	private static final int PORT = 8585;
	
	private static Mongo mongo;
	
	{ initialiseMongo(); }
	
	protected final Mongo mongo() {
		return mongo;
	}
	
	private static void initialiseMongo() {
		try {
			startMongo();
			mongo = new Mongo("localhost", PORT);
			clearDB();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	protected static void clearDB() {
		for (String name : mongo.getDatabaseNames()) {	
			mongo.dropDatabase(name);
		}
	}

	private static void startMongo() throws IOException {
		new File(MONGODB_TEST_DB).mkdirs();
		String path = "/Users/ops/dev-apps/mongodb/bin/mongod";
		File file = new File(path);
		if (! file.exists()) {
		    path = "/Library/mongodb/bin/mongod";
		    file = new File(path);
		    if (! file.exists()) {
		        path = "mongod";
		    }
		}
		Runtime.getRuntime().exec(path + " --port " + PORT + " --dbpath " + MONGODB_TEST_DB);
    }
}
