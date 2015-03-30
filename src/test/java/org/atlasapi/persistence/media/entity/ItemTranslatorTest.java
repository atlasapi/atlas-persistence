package org.atlasapi.persistence.media.entity;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.List;
import java.util.Set;

import org.atlasapi.media.entity.Actor;
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Clip;
import org.atlasapi.media.entity.ContentGroupRef;
import org.atlasapi.media.entity.CrewMember;
import org.atlasapi.media.entity.Encoding;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Location;
import org.atlasapi.media.entity.Policy;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.ReleaseDate;
import org.atlasapi.media.entity.TopicRef;
import org.atlasapi.media.entity.Version;
import org.joda.time.Duration;
import org.joda.time.LocalDate;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.intl.Countries;
import com.metabroadcast.common.persistence.MongoTestHelper;
import com.metabroadcast.common.persistence.mongo.MongoQueryBuilder;
import com.metabroadcast.common.time.Clock;
import com.metabroadcast.common.time.SystemClock;
import com.mongodb.BasicDBList;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

import junit.framework.TestCase;

public class ItemTranslatorTest extends TestCase {
	
	private final Clock clock = new SystemClock();
    private final ItemTranslator itemTranslator = new ItemTranslator(new SubstitutionTableNumberCodec());
    
    @SuppressWarnings("unchecked")
    public void testConvertFromItem() throws Exception {
        Item item = new Item("canonicalUri", "curie", Publisher.BBC);
        item.setTitle("title");
        item.setReleaseDates(Lists.newArrayList(new ReleaseDate(new LocalDate(2010, 3, 20),
            Countries.ALL, ReleaseDate.ReleaseType.GENERAL)));
        
        Location loc = new Location();
        loc.setAvailable(true);
        
        Encoding enc = new Encoding();
        enc.setAdvertisingDuration(1);
        enc.addAvailableAt(loc);
        
        Duration duration = Duration.standardSeconds(1);
        Broadcast br = new Broadcast("channel", clock.now(), duration);
        br.setScheduleDate(new LocalDate(2010, 3, 20));
        
        Version version = new Version();
        version.setDuration(duration);
        version.addManifestedAs(enc);
        version.addBroadcast(br);
        item.addVersion(version);
        
        Set<String> tags = Sets.newHashSet();
        tags.add("tag");
        item.setTags(tags);
        
        TopicRef topic1 = new TopicRef(1l, 0.01f, true, TopicRef.Relationship.ABOUT);
        TopicRef topic2 = new TopicRef(2l, 0.02f, false, TopicRef.Relationship.ABOUT);
        item.setTopicRefs(ImmutableList.of(topic1, topic2));
        
        DBObject dbObject = itemTranslator.toDBObject(null, item);
        
        assertEquals("canonicalUri", dbObject.get(IdentifiedTranslator.ID));
        assertEquals("title", dbObject.get("title"));
        
        List<String> t = (List<String>) dbObject.get("tags");
        assertFalse(t.isEmpty());
        for (String tag: t) {
            assertTrue(tags.contains(tag));
        }
        
        BasicDBList vs = (BasicDBList) dbObject.get("versions");
        assertEquals(1, vs.size());
        DBObject v = (DBObject) vs.get(0);
        assertEquals(version.getDuration(), v.get("duration"));
        
        BasicDBList bs = (BasicDBList) v.get("broadcasts");
        assertEquals(1, bs.size());
        DBObject b = (DBObject) bs.get(0);
        assertEquals(br.getScheduleDate().toString(), b.get("scheduleDate"));
        
        BasicDBList ma = (BasicDBList) v.get("manifestedAs");
        assertEquals(1, ma.size());
        DBObject e = (DBObject) ma.get(0);
        assertEquals(enc.getAdvertisingDuration(), e.get("advertisingDuration"));
        
        BasicDBList ls = (BasicDBList) e.get("availableAt");
        assertEquals(1, ls.size());
        DBObject l = (DBObject) ls.get(0);
        assertEquals(loc.getAvailable(), l.get("available"));
        
        List<DBObject> ts = (List<DBObject>) dbObject.get("topics");
		assertEquals(2, ts.size());
        DBObject t1 = (DBObject) ts.iterator().next();
        assertEquals(topic1.getTopic(), t1.get("topic"));
        assertThat(topic1.getWeighting().doubleValue(), is(closeTo((Double)t1.get("weighting"), 0.0001)));
        assertEquals(topic1.isSupervised(), t1.get("supervised"));
        assertEquals(topic1.getRelationship().name(), t1.get("relationship"));
    }
    
    public void testConvertToItem() throws Exception {
    	MongoTestHelper.ensureMongoIsRunning();
        DBCollection collection = MongoTestHelper.anEmptyTestDatabase().collection("test");
        
        Item item = new Item("canonicalUri", "curie", Publisher.BBC);
        item.setTitle("title");
        item.setReleaseDates(Lists.newArrayList(new ReleaseDate(new LocalDate(2010, 3, 20),
            Countries.ALL, ReleaseDate.ReleaseType.GENERAL)));

        Location loc = new Location();
        loc.setAvailable(true);
        
        Encoding enc = new Encoding();
        enc.setAdvertisingDuration(1);
        enc.addAvailableAt(loc);
        
        Duration duration = Duration.standardSeconds(1);
        
        Broadcast br = new Broadcast("channel", clock.now(), duration);
        br.setScheduleDate(new LocalDate(2010, 3, 20));
        
        Version version = new Version();
        version.setDuration(duration);
        version.addManifestedAs(enc);
        version.addBroadcast(br);
        item.addVersion(version);
        
        Actor actor = Actor.actor("an_id", "blah", "some guy", Publisher.BBC);
        item.addPerson(actor);
        
        Set<String> tags = Sets.newHashSet();
        tags.add("tag");
        item.setTags(tags);
        
        TopicRef topic1 = new TopicRef(1l, 0.01f, true, TopicRef.Relationship.ABOUT);
        TopicRef topic2 = new TopicRef(2l, 0.02f, false, TopicRef.Relationship.ABOUT);
        item.setTopicRefs(ImmutableList.of(topic1, topic2));
        
        ContentGroupRef contentGroup1 = new ContentGroupRef(1L, "uri");
        item.addContentGroup(contentGroup1);
        
        DBObject dbObject = itemTranslator.toDBObject(null, item);
        
        collection.save(dbObject);
        Item i = itemTranslator.fromDBObject(collection.findOne(new MongoQueryBuilder().idEquals(item.getCanonicalUri()).build()), null);

        assertEquals(i.getCanonicalUri(), item.getCanonicalUri());
        assertEquals(i.getCurie(), item.getCurie());
        
        Set<String> t = i.getTags();
        for (String tag: t) {
            assertTrue(item.getTags().contains(tag));
        }
        
        Set<Version> vs = item.getVersions();
        assertEquals(1, vs.size());
        Version v = vs.iterator().next();
        assertEquals(version.getDuration(), v.getDuration());
        
        Set<Broadcast> bs = v.getBroadcasts();
        assertEquals(1, bs.size());
        Broadcast b = bs.iterator().next();
        assertEquals(br.getScheduleDate(), b.getScheduleDate());
        
        Set<Encoding> ma = v.getManifestedAs();
        assertEquals(1, ma.size());
        Encoding e = ma.iterator().next();
        assertEquals(enc.getAdvertisingDuration(), e.getAdvertisingDuration());
        
        Set<Location> ls = e.getAvailableAt();
        assertEquals(1, ls.size());
        assertEquals(loc.getAvailable(), ls.iterator().next().getAvailable());
        
        List<CrewMember> people = i.people();
        assertEquals(1, people.size());
        assertEquals("some guy", ((Actor) Iterables.getFirst(people, null)).character());
        
        assertEquals(item.getTopicRefs(), i.getTopicRefs());
        
        assertEquals(item.getContentGroupRefs(), i.getContentGroupRefs());
    }
    
    @SuppressWarnings("unchecked")
    public void testRemovesLastUpdatedFromClipsForHashcode() {
        
        Item item = new Item("testUri", "testCurie", Publisher.BBC);
        createModel(item);
        
        Clip clip = new Clip();
        createModel(clip);
        item.addClip(clip);
        
        DBObject dbo = itemTranslator.toDB(item);
        itemTranslator.removeFieldsForHash(dbo);
        
        assertTimesAreNull(dbo);
        
        DBObject clipDbo = Iterables.getOnlyElement((Iterable<DBObject>) dbo.get("clips"));
        assertTimesAreNull(clipDbo);
    }

    @SuppressWarnings("unchecked")
    public void assertTimesAreNull(DBObject dbo) {
        assertNull(dbo.get(DescribedTranslator.LAST_FETCHED_KEY));
        assertNull(dbo.get(DescribedTranslator.THIS_OR_CHILD_LAST_UPDATED_KEY));
        assertLastUpdatedNull(dbo);
        
        assertNull(Iterables.getOnlyElement((Iterable<DBObject>) dbo.get("people")).get(IdentifiedTranslator.LAST_UPDATED));
        
        DBObject versionDbo = Iterables.getOnlyElement((Iterable<DBObject>) dbo.get("versions"));
        assertLastUpdatedNull(versionDbo);
        
        DBObject broadcastDbo = Iterables.getOnlyElement((Iterable<DBObject>) versionDbo.get("broadcasts"));
        assertLastUpdatedNull(broadcastDbo);
        
        DBObject encodingDbo = Iterables.getOnlyElement((Iterable<DBObject>) versionDbo.get("manifestedAs"));
        assertLastUpdatedNull(encodingDbo);
        
        DBObject locationDbo = Iterables.getOnlyElement((Iterable<DBObject>) encodingDbo.get("availableAt"));
        assertLastUpdatedNull(locationDbo);
        
        DBObject policyDbo = (DBObject) locationDbo.get("policy");
        assertLastUpdatedNull(policyDbo);
    }
    
    public void assertLastUpdatedNull(DBObject dbo) {
        assertNull(dbo.get(IdentifiedTranslator.LAST_UPDATED));
    }

    public void createModel(Item item) {
        item.setLastUpdated(clock.now());
        item.setThisOrChildLastUpdated(clock.now());
        item.setLastFetched(clock.now());
        
        Actor person = new Actor("personUri","personCurie",Publisher.BBC);
        person.setLastUpdated(clock.now());
        item.addPerson(person);
        
        Version version = new Version(); 
        version.setLastUpdated(clock.now());
        item.addVersion(version);
        
        Broadcast broadcast = new Broadcast("http://www.bbc.co.uk/bbcone", clock.now(), clock.now());
        broadcast.setLastUpdated(clock.now());
        version.addBroadcast(broadcast);
        
        Encoding encoding = new Encoding();
        encoding.setLastUpdated(clock.now());
        version.addManifestedAs(encoding);
        
        Location location = new Location();
        location.setLastUpdated(clock.now());
        encoding.addAvailableAt(location);
        
        Policy policy = new Policy();
        policy.setAvailabilityStart(clock.now());
        policy.setLastUpdated(clock.now());
        location.setPolicy(policy);
    }
}
