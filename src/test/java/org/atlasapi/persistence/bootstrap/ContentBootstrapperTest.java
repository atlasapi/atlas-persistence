package org.atlasapi.persistence.bootstrap;

import java.util.Iterator;
import java.util.List;

import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.listing.ContentLister;
import org.atlasapi.persistence.content.listing.ContentListingCriteria;
import org.jmock.Mockery;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class ContentBootstrapperTest {
    
    private final Mockery context = new Mockery();
    private final Item item1 = new Item("1", "1", Publisher.ARCHIVE_ORG);
    private final Item item2 = new Item("2", "2", Publisher.ARCHIVE_ORG);
    private final Item item3 = new Item("3", "3", Publisher.ARCHIVE_ORG);
    private final ContentLister lister1 = new ContentLister() {
        
        List<Content> contents = ImmutableList.<Content>of(item1, item2);
        
        @Override
        public Iterator<Content> listContent(ContentListingCriteria criteria) {
            return contents.iterator();
        }
    };
    private final ContentLister lister2 = new ContentLister() {
        
        List<Content> contents = ImmutableList.<Content>of(item3);
        
        @Override
        public Iterator<Content> listContent(ContentListingCriteria criteria) {
            return contents.iterator();
        }
    };
    private ContentBootstrapper bootstrapper = new ContentBootstrapper().withContentListers(lister1, lister2);
    
    @Test
    public void testListenerIsCalled() throws Exception {
        ChangeListener listener = mock(ChangeListener.class);
        
        assertTrue(bootstrapper.loadAllIntoListener(listener));
        verify(listener).beforeChange();
        verify(listener).onChange(ImmutableList.of(item1, item2));
        verify(listener).onChange(ImmutableList.of(item3));
        verify(listener).afterChange();
    }
    
    @Test
    public void testListenerIsNotCalledConcurrently() throws Exception {
        final CountDownLatch before = new CountDownLatch(1);
        final CountDownLatch after = new CountDownLatch(1);
        final ChangeListener listener = mock(ChangeListener.class);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        
        doAnswer(new Answer() {
            
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                before.countDown();
                after.await();
                return null;
            }
        }).when(listener).beforeChange();
        
        executor.submit(new Runnable() {
            
            @Override
            public void run() {
                bootstrapper.loadAllIntoListener(listener);
            }
        });
        
        before.await();
        assertFalse(bootstrapper.loadAllIntoListener(listener));
        after.countDown();
        
    }
}
