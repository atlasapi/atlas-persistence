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

package org.atlasapi.persistence.content.mongo;

import static org.atlasapi.content.criteria.ContentQueryBuilder.query;
import static org.atlasapi.content.criteria.attribute.Attributes.ENCODING_DATA_CONTAINER_FORMAT;
import static org.atlasapi.content.criteria.attribute.Attributes.LOCATION_AVAILABLE;
import static org.atlasapi.content.criteria.attribute.Attributes.LOCATION_TRANSPORT_TYPE;
import static org.atlasapi.content.criteria.attribute.Attributes.VERSION_DURATION;
import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.List;

import org.atlasapi.content.criteria.ContentQuery;
import org.atlasapi.media.TransportType;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Encoding;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Location;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Version;
import org.joda.time.Duration;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.metabroadcast.common.media.MimeType;

public class QueryResultCheckerTest {

	private QueryResultTrimmer trimmer;
	private Version shortVersion;
	private Version longVersion;
	
	private Location availableLocation;
	private Location unavailableLocation;
	private Location streamingLocation;

	@Before
	public void setUp() throws Exception {
		
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
	
	@Test
	public void testTrimmingVersions() throws Exception {
		Item item = publicItem();
		
		item.addVersion(shortVersion);
		item.addVersion(longVersion);
		
		trimmer.trim(Collections.singletonList(item), query().equalTo(VERSION_DURATION, 10).build(), true);
		
		assertEquals(Sets.newHashSet(longVersion), item.getVersions());
	
		assertEquals(Collections.emptyList(), trimmer.trim(Collections.singletonList(item), query().equalTo(ENCODING_DATA_CONTAINER_FORMAT, MimeType.VIDEO_XMATROSKA).build(), true));
	}
	
	@Test
	public void testTrimmingContainers() throws Exception {
		Item item = publicItem();
		
		item.addVersion(shortVersion);
		item.addVersion(longVersion);
		
		Container<Item> container = publicContainer();
		container.setContents(item);
		
		List<Container<Item>> found = trimmer.trim(Collections.singletonList(container), query().equalTo(VERSION_DURATION, 10).build(), false);
		
		assertEquals(ImmutableList.of(container), found);
		
		assertEquals(Sets.newHashSet(longVersion), item.getVersions());
		
		assertEquals(Collections.emptyList(), trimmer.trim(Collections.singletonList(item), query().equalTo(ENCODING_DATA_CONTAINER_FORMAT, MimeType.VIDEO_XMATROSKA).build(), true));
	}
	
	@Test
	public void testTrimmingALocationByAvailablity() {
		Item item = publicItem();
		
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
	
	@Test
	public void testTrimmingALocationByTransportType() {
		Item item = publicItem();
		
		shortVersion.addManifestedAs(encodingWithLocation(availableLocation));
		longVersion.addManifestedAs(encodingWithLocation(streamingLocation));
		
		item.addVersion(shortVersion);
		item.addVersion(longVersion);
		
		trimmer.trim(Collections.singletonList(item), query().equalTo(LOCATION_TRANSPORT_TYPE, TransportType.STREAM).build(), true);
		assertEquals(Sets.newHashSet(longVersion), item.getVersions());
	}
	
	@Test
	public void testCompoundQueries() throws Exception {
		Item item1 = publicItem();
		item1.setIsLongForm(true);
		
		Encoding encoding = encodingWithLocation(availableLocation);
		shortVersion.addManifestedAs(encoding);
		
		longVersion.addManifestedAs(encodingWithLocation(unavailableLocation));
		
		item1.addVersion(shortVersion);
		item1.addVersion(longVersion);
		
		ContentQuery query = query().equalTo(LOCATION_AVAILABLE, true).equalTo(ENCODING_DATA_CONTAINER_FORMAT, MimeType.TEXT_HTML).build();
		assertEquals(Collections.emptyList(), trimmer.trim(Collections.singletonList(item1), query, true));
		assertEquals(Collections.singletonList(item1), trimmer.trim(Collections.singletonList(item1), query, false));
	}

	private Encoding encodingWithLocation(Location location) {
		Encoding encoding = new Encoding();
		encoding.addAvailableAt(location);
		return encoding;
	}
	
	private Item publicItem() {
		Item item = new Item();
		item.setPublisher(Publisher.YOUTUBE);
		return item;
	}
	
	private Container<Item> publicContainer() {
		Container<Item> container = new Container<Item>();
		container.setPublisher(Publisher.YOUTUBE);
		return container;
	}
}
