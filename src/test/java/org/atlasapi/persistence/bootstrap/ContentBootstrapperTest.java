package org.atlasapi.persistence.bootstrap;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.atlasapi.media.content.Content;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentCategory;
import org.atlasapi.persistence.content.listing.ContentLister;
import org.atlasapi.persistence.content.listing.ContentListingCriteria;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;

public class ContentBootstrapperTest {
    
    private final Item item1 = new Item("1", "1", Publisher.ARCHIVE_ORG);
    private final Item item2 = new Item("2", "2", Publisher.ARCHIVE_ORG);
    private final Item item3 = new Item("3", "3", Publisher.ARCHIVE_ORG);
    private final ContentLister lister1 = new ContentLister() {
        
        List<Content> contents = ImmutableList.<Content>of(item1, item2);
        
        @Override
        public Iterator<Content> listContent(ContentListingCriteria criteria) {
            if (criteria.getCategories().containsAll(ContentCategory.ITEMS)) {
                return contents.iterator();
            }
            return Iterators.emptyIterator();
        }
    };
    private final ContentLister lister2 = new ContentLister() {
        
        List<Content> contents = ImmutableList.<Content>of(item3);
        
        @Override
        public Iterator<Content> listContent(ContentListingCriteria criteria) {
            if (criteria.getCategories().containsAll(ContentCategory.ITEMS)) {
                return contents.iterator();
            }
            return Iterators.emptyIterator();
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
        
        doAnswer(new Answer<Object>() {
            
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
