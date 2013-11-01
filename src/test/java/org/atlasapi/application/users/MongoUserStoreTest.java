package org.atlasapi.application.users;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

import java.util.Set;

import org.atlasapi.application.Application;
import org.atlasapi.media.common.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.application.ApplicationStore;
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
    }
    
    @Test
    public void testTranslator() {
      
        final Set<String> appSlugs = ImmutableSet.of("app1", "app2");
        final Set<Id> appIds = ImmutableSet.of(Id.valueOf(5000), Id.valueOf(6000));
        final Set<Publisher> sources = ImmutableSet.of(Publisher.BBC, Publisher.YOUTUBE);
        final UserRef userRef = new UserRef(1234L, UserNamespace.TWITTER, "test");
        
        ApplicationStore applicationStore = mock(ApplicationStore.class);
        when(applicationStore.applicationIdsForSlugs(appSlugs)).thenReturn(appIds);
        when(applicationStore.applicationFor(Id.valueOf(5000))).thenReturn(
                Optional.of(Application.builder().withSlug("app1").build()));
        when(applicationStore.applicationFor(Id.valueOf(6000))).thenReturn(
                Optional.of(Application.builder().withSlug("app2").build()));
        store = new MongoUserStore(adminMongo, applicationStore);
        User user = User.builder()
                .withId(Id.valueOf(5000L))
                .withUserRef(userRef)
                .withScreenName("test")
                .withFullName("test test")
                .withCompany("the company")
                .withEmail("me@example.com")
                .withWebsite("http://example.com")
                .withProfileImage("http://example.com/image")
                .withApplicationIds(appIds)
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
        assertTrue(found.get().getApplicationIds().containsAll(appIds));
        assertTrue(found.get().getSources().containsAll(sources));
        assertEquals(found.get().getRole(), Role.ADMIN);
        assertTrue(found.get().isProfileComplete());
        
    }
}
