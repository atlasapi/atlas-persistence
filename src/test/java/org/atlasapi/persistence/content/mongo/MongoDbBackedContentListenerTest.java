package org.atlasapi.persistence.content.mongo;

import java.util.List;

import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentListener;
import org.atlasapi.persistence.content.RetrospectiveContentLister;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JMock;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.google.common.collect.ImmutableList;

@RunWith(JMock.class)
public class MongoDbBackedContentListenerTest  {
   
	private final Mockery context = new Mockery();
	
	private RetrospectiveContentLister store = context.mock(RetrospectiveContentLister.class);
    private ContentListener listener = context.mock(ContentListener.class);
   
    private MongoDbBackedContentBootstrapper bootstrapper = new MongoDbBackedContentBootstrapper(listener, store);
    
    private final Item item1 = new Item("1", "1", Publisher.ARCHIVE_ORG);
    private final Item item2 = new Item("2", "2", Publisher.ARCHIVE_ORG);
    private final Item item3 = new Item("3", "3", Publisher.ARCHIVE_ORG);
    
    @Test
    public void testShouldLoadItems() throws Exception {
        bootstrapper.setBatchSize(2);
        
        final List<Item> items1 = ImmutableList.of(item1, item2);
        final List<Item> items2 = ImmutableList.<Item>of(item3);
        
        context.checking(new Expectations() {{
            one(store).listAllRoots(null, 2); will(returnValue(items1));
            one(store).listAllRoots(item2.getCanonicalUri(), 2); will(returnValue(items2));
        }});
        
        context.checking(new Expectations() {{
            one(listener).itemChanged(ImmutableList.of(item1, item2), ContentListener.ChangeType.BOOTSTRAP);
            one(listener).itemChanged(ImmutableList.of(item3), ContentListener.ChangeType.BOOTSTRAP);
        }});
        
       bootstrapper.loadAllItems();
    }
}
