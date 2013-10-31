package org.atlasapi.application.users;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Set;

import org.atlasapi.media.common.Id;
import org.atlasapi.media.entity.Publisher;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.persistence.MongoTestHelper;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.social.model.UserRef;
import com.metabroadcast.common.social.model.UserRef.UserNamespace;


public class MongoUserStoreTest {
    private DatabasedMongo adminMongo;
    private UserStore store;
    
    @Before
    public void setup() {
        adminMongo = MongoTestHelper.anEmptyTestDatabase();
        store = new MongoUserStore(adminMongo);
    }
    
    @Test
    public void testTranslator() {
        
        final Set<String> appSlugs = ImmutableSet.of("app1", "app2");
        final Set<Publisher> sources = ImmutableSet.of(Publisher.BBC, Publisher.YOUTUBE);
        final UserRef userRef = new UserRef(1234L, UserNamespace.TWITTER, "test");
        User user = User.builder()
                .withId(Id.valueOf(5000L))
                .withUserRef(userRef)
                .withScreenName("test")
                .withFullName("test test")
                .withCompany("the company")
                .withEmail("me@example.com")
                .withWebsite("http://example.com")
                .withProfileImage("http://example.com/image")
                .withApplicationSlugs(appSlugs)
                .withSources(sources)
                .withRole(Role.ADMIN)
                .withProfileComplete(true)
                .build();
        store.store(user);
        
        Optional<User> found = store.userForId(Id.valueOf(5000L));
        if (!found.isPresent()) {
            fail("User not found in store");
            return;
        }
        assertEquals(found.get().getId().longValue(), 5000L);
        assertEquals(found.get().getUserRef(), userRef);
        assertEquals(found.get().getScreenName(), "test");
        assertEquals(found.get().getFullName(), "test test");
        assertEquals(found.get().getCompany(), "the company");
        assertEquals(found.get().getEmail(), "me@example.com");
        assertEquals(found.get().getProfileImage(), "http://example.com/image");
        assertTrue(found.get().getApplicationSlugs().containsAll(appSlugs));
        assertTrue(found.get().getSources().containsAll(sources));
        assertEquals(found.get().getRole(), Role.ADMIN);
        assertTrue(found.get().isProfileComplete());
        
    }
}
