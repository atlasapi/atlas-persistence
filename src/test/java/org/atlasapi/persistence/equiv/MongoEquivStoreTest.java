package org.atlasapi.persistence.equiv;

import static org.junit.Assert.assertEquals;

import java.util.Collections;

import org.atlasapi.media.entity.Equiv;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.persistence.MongoTestHelper;

public class MongoEquivStoreTest {
	
	private final MongoEquivStore store = new MongoEquivStore(MongoTestHelper.anEmptyTestDatabase());
	
	@Test
	public void testAnEmptyStore() throws Exception {
		assertEquals(Collections.<String>emptySet(), store.get(ImmutableSet.of("any")));
	}
	
	@Test
	public void testASingleEquivalence() throws Exception {
		store.store(new Equiv("a", "b"));
		assertEquals(ImmutableSet.of("b"), store.get(ImmutableSet.of("a")));
		assertEquals(ImmutableSet.of("a"), store.get(ImmutableSet.of("b")));
	}
	
	@Test
	public void testATransitiveEquivalence() throws Exception {
		store.store(new Equiv("a", "b"));
		store.store(new Equiv("c", "b"));
		store.store(new Equiv("c", "d"));
		assertEquals(ImmutableSet.of("b", "c", "d"), store.get(ImmutableSet.of("a")));
		assertEquals(ImmutableSet.of("a", "b", "d"), store.get(ImmutableSet.of("c")));
	}
	
	@Test
	public void testACycle() throws Exception {
		store.store(new Equiv("a", "b"));
		store.store(new Equiv("b", "c"));
		store.store(new Equiv("c", "a"));
		assertEquals(ImmutableSet.of("b", "c"), store.get(ImmutableSet.of("a")));
	}
}
