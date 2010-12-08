package org.atlasapi.persistence.content.mongo;

import java.util.List;

import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Playlist;
import org.atlasapi.persistence.content.ContentListener;
import org.atlasapi.persistence.content.RetrospectiveContentLister;
import org.atlasapi.persistence.testing.DummyContentData;
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
    private DummyContentData data = new DummyContentData();
    
    @Test
    public void testShouldLoadItems() throws Exception {
        bootstrapper.setBatchSize(2);
        
        final List<Item> items = ImmutableList.of(data.eggsForBreakfast, data.englishForCats, data.everyoneNeedsAnEel);
        
        context.checking(new Expectations() {{
            one(store).listAllItems(); will(returnValue(items.iterator()));
        }});
        
        context.checking(new Expectations() {{
            one(listener).itemChanged(ImmutableList.of(data.eggsForBreakfast, data.englishForCats), ContentListener.changeType.BOOTSTRAP);
            one(listener).itemChanged(ImmutableList.of(data.everyoneNeedsAnEel), ContentListener.changeType.BOOTSTRAP);
        }});
        
        bootstrapper.loadAllItems();
    }
    
    @Test
    public void testShouldLoadBrands() throws Exception {

    	bootstrapper.setBatchSize(2);
        
        final List<Playlist> playlists = ImmutableList.of(data.eastenders, data.goodEastendersEpisodes, data.dispatches);
        
        context.checking(new Expectations() {{
            one(store).listAllPlaylists(); will(returnValue(playlists.iterator()));
        }});
        
        context.checking(new Expectations() {{
            one(listener).brandChanged(ImmutableList.of(data.eastenders, data.dispatches), ContentListener.changeType.BOOTSTRAP);
        }});
        
        bootstrapper.loadAllBrands();
    }
}
