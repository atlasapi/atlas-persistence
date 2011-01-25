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
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Item;
import org.atlasapi.persistence.content.ContentListener;
import org.atlasapi.persistence.content.RetrospectiveContentLister;
import org.springframework.beans.factory.InitializingBean;

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
            log.info("Bootstrapping top level content");
        }
        loadAllBrands();
        loadAllItems();
    }

    public void loadAllItems() {
        loadAll(Item.class);
    }

    public void loadAllBrands() {
    	loadAll(Container.class);
    }
    
    @SuppressWarnings("unchecked")
	private void loadAll(Class<? extends Content> filter) {
        while(true) {
        	String fromId = null;
        	List<Content> roots = contentStore.listAllRoots(fromId, batchSize);
            
        	List<? extends Content> batch = ImmutableList.copyOf(Iterables.filter(roots, filter));
        	
        	if (Item.class.isAssignableFrom(filter)) {
        		contentListener.itemChanged((List<Item>) batch, ContentListener.ChangeType.BOOTSTRAP);
        	}
        	
        	if (Container.class.isAssignableFrom(filter)) {
        		contentListener.brandChanged((List<Container<?>>) batch, ContentListener.ChangeType.BOOTSTRAP);
        	}
        	
            if (roots.isEmpty() || roots.size() < batchSize) {
            	break;
            }
            fromId =  Iterables.getLast(roots).getCanonicalUri();
        }
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }
}
