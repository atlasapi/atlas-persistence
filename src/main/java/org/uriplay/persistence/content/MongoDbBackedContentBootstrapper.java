package org.uriplay.persistence.content;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jherd.util.Selection;
import org.springframework.beans.factory.InitializingBean;
import org.uriplay.media.entity.Brand;
import org.uriplay.media.entity.Item;
import org.uriplay.media.entity.Playlist;

import com.google.common.collect.Lists;

public class MongoDbBackedContentBootstrapper implements InitializingBean {
    private static final Log log = LogFactory.getLog(MongoDbBackedContentBootstrapper.class);
    private static final int BATCH_SIZE = 100;

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
        do {
            Selection selection = new Selection(offset, batchSize);
            items = contentStore.listAllItems(selection);

            if (!items.isEmpty()) {
                contentListener.itemChanged(items, ContentListener.changeType.BOOTSTRAP);
            }
            offset += items.size();
            
            if (log.isInfoEnabled()) {
                log.info("Bootstrapped "+items.size()+" items");
            }
        } while (items.size() == batchSize);
    }

    public void loadAllBrands() {
        int offset = 0;

        List<Playlist> playlists = Lists.newArrayList();
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
            if (log.isInfoEnabled()) {
                log.info("Bootstrapped "+brands.size()+" brands");
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
