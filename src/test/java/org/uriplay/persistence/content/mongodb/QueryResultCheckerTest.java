/* Copyright 2010 Meta Broadcast Ltd

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License. You may
obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License. */

package org.uriplay.persistence.content.mongodb;

import static org.uriplay.content.criteria.attribute.Attributes.BRAND_TITLE;
import static org.uriplay.content.criteria.attribute.Attributes.ENCODING_DATA_CONTAINER_FORMAT;
import static org.uriplay.content.criteria.attribute.Attributes.LOCATION_AVAILABLE;
import static org.uriplay.content.criteria.attribute.Attributes.LOCATION_TRANSPORT_TYPE;
import static org.uriplay.content.criteria.attribute.Attributes.VERSION_DURATION;

import java.util.Collections;

import org.jmock.integration.junit3.MockObjectTestCase;
import org.joda.time.Duration;
import org.uriplay.content.criteria.ContentQuery;
import org.uriplay.media.TransportType;
import org.uriplay.media.entity.Encoding;
import org.uriplay.media.entity.Item;
import org.uriplay.media.entity.Location;
import org.uriplay.media.entity.Version;
import org.uriplay.persistence.content.mongodb.QueryResultTrimmer;

import com.google.common.collect.Sets;

import static org.uriplay.content.criteria.ContentQueryBuilder.query;

public class QueryResultCheckerTest extends MockObjectTestCase {

	private QueryResultTrimmer trimmer;
	private Version shortVersion;
	private Version longVersion;
	
	private Location availableLocation;
	private Location unavailableLocation;
	private Location streamingLocation;

	@Override
	protected void setUp() throws Exception {
		
		super.setUp();
		trimmer = new QueryResultTrimmer();
		
		shortVersion = new Version();
		shortVersion.setDuration(Duration.standardSeconds(1));
		longVersion = new Version();
		longVersion.setDuration(Duration.standardSeconds(10));
		
		availableLocation = new Location();
		availableLocation.setAvailable(true);
		
		unavailableLocation = new Location();
		unavailableLocation.setAvailable(false);
		
		streamingLocation = new Location();
		streamingLocation.setTransportType(TransportType.STREAM);
	}
	
	public void testAQueryThatShouldNotBeTrimmed() throws Exception {
		
		Item item = new Item();
		
		item.addVersion(shortVersion);
		item.addVersion(longVersion);
		
		trimmer.trim(Collections.singletonList(item), query().equalTo(BRAND_TITLE, "test").build(), true);
		assertEquals(Sets.newHashSet(shortVersion, longVersion), item.getVersions());
	}
	
	public void testTrimmingVersions() throws Exception {
		Item item = new Item();
		
		item.addVersion(shortVersion);
		item.addVersion(longVersion);
		
		trimmer.trim(Collections.singletonList(item), query().equalTo(VERSION_DURATION, 10).build(), true);
		
		assertEquals(Sets.newHashSet(longVersion), item.getVersions());
	
		assertEquals(Collections.emptyList(), trimmer.trim(Collections.singletonList(item), query().equalTo(ENCODING_DATA_CONTAINER_FORMAT, "test").build(), true));
	}
	
	public void testTrimmingALocationByAvailablity() {
		Item item = new Item();
		
		shortVersion.addManifestedAs(encodingWithLocation(availableLocation));
		longVersion.addManifestedAs(encodingWithLocation(unavailableLocation));
		
		item.addVersion(shortVersion);
		item.addVersion(longVersion);

		// should match both
		trimmer.trim(Collections.singletonList(item), query().equalTo(LOCATION_AVAILABLE, true, false).build(), true);
		assertEquals(Sets.newHashSet(shortVersion, longVersion), item.getVersions());
		
		// should match one
		trimmer.trim(Collections.singletonList(item), query().equalTo(LOCATION_AVAILABLE, true).build(), true);
		assertEquals(Sets.newHashSet(shortVersion), item.getVersions());
	
	}
	
	public void testTrimmingALocationByTransportType() {
		Item item = new Item();
		
		shortVersion.addManifestedAs(encodingWithLocation(availableLocation));
		longVersion.addManifestedAs(encodingWithLocation(streamingLocation));
		
		item.addVersion(shortVersion);
		item.addVersion(longVersion);
		
		trimmer.trim(Collections.singletonList(item), query().equalTo(LOCATION_TRANSPORT_TYPE, TransportType.STREAM).build(), true);
		assertEquals(Sets.newHashSet(longVersion), item.getVersions());
	}
	
	public void testCompoundQueries() throws Exception {
		Item item1 = new Item();
		item1.setIsLongForm(true);
		
		Encoding encoding = encodingWithLocation(availableLocation);
		shortVersion.addManifestedAs(encoding);
		
		longVersion.addManifestedAs(encodingWithLocation(unavailableLocation));
		
		item1.addVersion(shortVersion);
		item1.addVersion(longVersion);
		
		ContentQuery query = query().equalTo(LOCATION_AVAILABLE, true).equalTo(ENCODING_DATA_CONTAINER_FORMAT, "html").build();
		assertEquals(Collections.emptyList(), trimmer.trim(Collections.singletonList(item1), query, true));
		assertEquals(Collections.singletonList(item1), trimmer.trim(Collections.singletonList(item1), query, false));
	}

	private Encoding encodingWithLocation(Location location) {
		Encoding encoding = new Encoding();
		encoding.addAvailableAt(location);
		return encoding;
	}
}
