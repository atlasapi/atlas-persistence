package org.atlasapi.persistence.equiv;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.Collections;
import java.util.Set;

import org.atlasapi.media.entity.Equiv;
import org.atlasapi.persistence.equiv.MongoEquivStore;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.metabroadcast.common.persistence.MongoTestHelper;
import com.mongodb.Mongo;

@SuppressWarnings("unchecked")
public class MongoEquivStoreTest {
	
	private final Mongo mongo = MongoTestHelper.anEmptyMongo();
	private final MongoEquivStore store = new MongoEquivStore(mongo, "testing");
	
	@Test
	public void testAnEmptyStore() throws Exception {
		assertThat(store.get(ImmutableSet.of("any")), is(Collections.<String>emptySet()));
	}
	
	@Test
	public void testASingleEquivalence() throws Exception {
		store.store(new Equiv("a", "b"));
		assertThat(store.get(ImmutableSet.of("a")), is((Set) Sets.newHashSet("b")));
		assertThat(store.get(ImmutableSet.of("b")), is((Set) Sets.newHashSet("a")));
	}
	
	@Test
	public void testATransitiveEquivalence() throws Exception {
		store.store(new Equiv("a", "b"));
		store.store(new Equiv("c", "b"));
		store.store(new Equiv("c", "d"));
		assertThat(store.get(ImmutableSet.of("a")), is((Set) Sets.newHashSet("b", "c", "d")));
		assertThat(store.get(ImmutableSet.of("c")), is((Set) Sets.newHashSet("a", "b", "d")));
	}
	
	@Test
	public void testACycle() throws Exception {
		store.store(new Equiv("a", "b"));
		store.store(new Equiv("b", "c"));
		store.store(new Equiv("c", "a"));
		assertThat(store.get(ImmutableSet.of("a")), is((Set) Sets.newHashSet("b", "c")));
	}
}
