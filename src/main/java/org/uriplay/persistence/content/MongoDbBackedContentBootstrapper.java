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

package org.uriplay.persistence.content;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.InitializingBean;
import org.uriplay.media.entity.Brand;
import org.uriplay.media.entity.Item;
import org.uriplay.media.entity.Playlist;

import com.google.common.collect.Lists;
import com.metabroadcast.common.query.Selection;

public class MongoDbBackedContentBootstrapper implements InitializingBean {
    private static final Log log = LogFactory.getLog(MongoDbBackedContentBootstrapper.class);
    private static final int BATCH_SIZE = 500;

    private final ContentListener contentListener;
    private final ContentStore contentStore;
    private int batchSize = BATCH_SIZE;

    public MongoDbBackedContentBootstrapper(ContentListener contentListener, ContentStore contentStore) {
        this.contentListener = contentListener;
        this.contentStore = contentStore;
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

        if (log.isInfoEnabled()) {
            log.info("Bootstrapping Brands");
        }
        loadAllItems();
    }

    public void loadAllItems() {
        int offset = 0;

        List<Item> items = Lists.newArrayList();
        int count = 0;
        do {
            Selection selection = new Selection(offset, batchSize);
            items = contentStore.listAllItems(selection);

            if (!items.isEmpty()) {
                contentListener.itemChanged(items, ContentListener.changeType.BOOTSTRAP);
            }
            offset += items.size();

            if (count > 10000) {
                if (log.isInfoEnabled()) {
                    log.info("Bootstrapped " + count + " items");
                }
                count = 0;
            } else {
                count = count + items.size();
            }
        } while (items.size() == batchSize);
    }

    public void loadAllBrands() {
        int offset = 0;

        List<Playlist> playlists = Lists.newArrayList();
        int count = 0;
        do {
            List<Brand> brands = Lists.newArrayList();
            Selection selection = new Selection(offset, batchSize);

            playlists = contentStore.listAllPlaylists(selection);
            for (Playlist playlist : playlists) {
                if (playlist instanceof Brand) {
                    brands.add((Brand) playlist);
                }
            }

            if (!playlists.isEmpty()) {
                contentListener.brandChanged(brands, ContentListener.changeType.BOOTSTRAP);
            }
            offset += playlists.size();
            if (count > 10000) {
                if (log.isInfoEnabled()) {
                    log.info("Bootstrapped " + count + " brands");
                }
                count = 0;
            } else {
                count = count + playlists.size();
            }
        } while (playlists.size() == batchSize);
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }
}
