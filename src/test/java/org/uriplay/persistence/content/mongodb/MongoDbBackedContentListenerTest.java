package org.uriplay.persistence.content.mongodb;

import java.util.List;

import org.jmock.Expectations;
import org.jmock.integration.junit3.MockObjectTestCase;
import org.uriplay.media.entity.Brand;
import org.uriplay.media.entity.Item;
import org.uriplay.media.entity.Playlist;
import org.uriplay.persistence.content.ContentListener;
import org.uriplay.persistence.content.ContentStore;
import org.uriplay.persistence.content.MongoDbBackedContentBootstrapper;
import org.uriplay.persistence.testing.DummyContentData;

import com.google.common.collect.Lists;
import com.metabroadcast.common.query.Selection;

public class MongoDbBackedContentListenerTest extends MockObjectTestCase {
    private ContentStore store = mock(ContentStore.class);
    private ContentListener listener = mock(ContentListener.class);
    private MongoDbBackedContentBootstrapper bootstrapper = new MongoDbBackedContentBootstrapper(listener, store);
    private DummyContentData data = new DummyContentData();
    
    private final List<Item> items1 = Lists.newArrayList();
    private final List<Item> items2 = Lists.newArrayList();
    
    private final List<Playlist> playlists1 = Lists.newArrayList();
    private final List<Playlist> playlists2 = Lists.newArrayList();
    
    private final List<Brand> brands1 = Lists.newArrayList();
    private final List<Brand> brands2 = Lists.newArrayList();
    
    public void testShouldLoadItems() throws Exception {
        bootstrapper.setBatchSize(2);
        
        items1.add(data.eggsForBreakfast);
        items1.add(data.englishForCats);

        items2.add(data.everyoneNeedsAnEel);
        
        checking(new Expectations() {{
            one(store).listAllItems(new Selection(0, 2)); will(returnValue(items1));
            one(store).listAllItems(new Selection(2, 2)); will(returnValue(items2));
        }});
        
        checking(new Expectations() {{
            one(listener).itemChanged(items1, ContentListener.changeType.BOOTSTRAP);
            one(listener).itemChanged(items2, ContentListener.changeType.BOOTSTRAP);
        }});
        
        bootstrapper.loadAllItems();
    }
    
    public void testShouldLoadBrands() throws Exception {
        bootstrapper.setBatchSize(2);
        
        playlists1.add(data.apprentice);
        playlists1.add(data.goodEastendersEpisodes);
        brands1.add(data.apprentice);
        
        playlists2.add(data.timeTeam);
        brands2.add(data.timeTeam);
        
        checking(new Expectations() {{
            one(store).listAllPlaylists(new Selection(0, 2)); will(returnValue(playlists1));
            one(store).listAllPlaylists(new Selection(2, 2)); will(returnValue(playlists2));
        }});
        
        checking(new Expectations() {{
            one(listener).brandChanged(brands1, ContentListener.changeType.BOOTSTRAP);
            one(listener).brandChanged(brands2, ContentListener.changeType.BOOTSTRAP);
        }});
        
        bootstrapper.loadAllBrands();
    }
}
