package org.atlasapi.persistence.shorturls;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import org.atlasapi.media.entity.Identified;

import com.google.common.base.Strings;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.persistence.mongo.MongoConstants;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;

public class MongoShortUrlSaver implements ShortUrlSaver {

	private final DBCollection shortUrls;

	public MongoShortUrlSaver(DatabasedMongo mongo) {
		shortUrls = mongo.collection("shortUrls");
	}

	@Override
	public void save(String shortUrl, Identified mapsTo) {
		checkArgument(!Strings.isNullOrEmpty(shortUrl), "Short url should not be blank");
		checkNotNull(mapsTo);
		shortUrls.save(new BasicDBObject(MongoConstants.ID, shortUrl).append("content", mapsTo.getCanonicalUri()));
	}
}
