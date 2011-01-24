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
        
        final List<Item> items1 = ImmutableList.of(data.eggsForBreakfast, data.englishForCats);
        final List<Item> items2 = ImmutableList.of(data.everyoneNeedsAnEel);
        
        context.checking(new Expectations() {{
            one(store).listItems(null, 2); will(returnValue(items1));
            one(store).listItems(data.englishForCats.getCanonicalUri(), 2); will(returnValue(items2));
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
        
        final List<Playlist> playlists1 = ImmutableList.of(data.eastenders, data.goodEastendersEpisodes);
        final List<Playlist> playlists2 = ImmutableList.<Playlist>of(data.dispatches);
        
        context.checking(new Expectations() {{
            one(store).listPlaylists(null, 2); will(returnValue(playlists1));
            one(store).listPlaylists(data.goodEastendersEpisodes.getCanonicalUri(), 2); will(returnValue(playlists2));
        }});
        
        context.checking(new Expectations() {{
            one(listener).brandChanged(ImmutableList.of(data.eastenders), ContentListener.changeType.BOOTSTRAP);
            one(listener).brandChanged(ImmutableList.of(data.dispatches), ContentListener.changeType.BOOTSTRAP);
        }});
        
        bootstrapper.loadAllBrands();
    }
}
