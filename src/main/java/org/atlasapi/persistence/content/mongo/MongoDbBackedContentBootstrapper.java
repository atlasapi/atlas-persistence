/* Copyright 2009 Meta Broadcast Ltd

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License. You may
obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License. */

package org.atlasapi.persistence.content.mongo;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Playlist;
import org.atlasapi.persistence.content.ContentListener;
import org.atlasapi.persistence.content.RetrospectiveContentLister;
import org.springframework.beans.factory.InitializingBean;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

public class MongoDbBackedContentBootstrapper implements InitializingBean {
	
    private static final Log log = LogFactory.getLog(MongoDbBackedContentBootstrapper.class);
    private static final int BATCH_SIZE = 100;

    private final ContentListener contentListener;
    private final RetrospectiveContentLister contentStore;
    private int batchSize = BATCH_SIZE;

    public MongoDbBackedContentBootstrapper(ContentListener contentListener, RetrospectiveContentLister contentLister) {
        this.contentListener = contentListener;
        this.contentStore = contentLister;
    }

    @Override
    public void afterPropertiesSet() {
        new Thread() {
            public void run() {
                loadAll();
            };
        }.start();
    }

    void loadAll() {
        if (log.isInfoEnabled()) {
            log.info("Bootstrapping Brands");
        }
        loadAllBrands();

//        if (log.isInfoEnabled()) {
//            log.info("Bootstrapping Items");
//        }
//        loadAllItems();
    }

    public void loadAllItems() {
        List<Item> items = ImmutableList.of();
        String fromId = null;
        do {
            items = contentStore.listItems(fromId, batchSize);
            load(items, new Function<List<Item>, Void>() {
                @Override
                public Void apply(List<Item> batch) {
                    contentListener.itemChanged(batch, ContentListener.changeType.BOOTSTRAP);
                    return null;
                }
            });
            Item lastItem = Iterables.getLast(items, null);
            fromId = lastItem != null ? lastItem.getCanonicalUri() : null;
        } while (! items.isEmpty() && items.size() >= batchSize);
    }

	private <T extends Content> void load(Iterable<T> contents, final Function<List<T>, Void> handler) {
	    handler.apply(ImmutableList.copyOf(contents));
	}

    public void loadAllBrands() {
        List<Playlist> playlists = ImmutableList.of();
        String fromId = null;
        do {
            playlists = contentStore.listPlaylists(fromId, batchSize);
            load(Iterables.filter(playlists, Brand.class), new Function<List<Brand>, Void>() {
                @Override
                public Void apply(List<Brand> batch) {
                    contentListener.brandChanged(batch, ContentListener.changeType.BOOTSTRAP);
                    return null;
                }
            });
            Playlist lastPlaylist = Iterables.getLast(playlists, null);
            fromId = lastPlaylist != null ? lastPlaylist.getCanonicalUri() : null;
        } while (! playlists.isEmpty() && playlists.size() >= batchSize);
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }
}
