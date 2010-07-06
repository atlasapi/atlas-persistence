package org.uriplay.persistence.system;

import java.util.Collections;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Required;
import org.uriplay.media.entity.Content;
import org.uriplay.media.entity.Item;
import org.uriplay.media.entity.Playlist;
import org.uriplay.persistence.content.ContentWriter;
import org.uriplay.persistence.servlet.ContentNotFoundException;

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

public class UriRefresher implements Runnable  {

	private static final Log log = LogFactory.getLog(UriRefresher.class);
	
	private Fetcher<Content> fetcher;
	private ContentWriter contentStore;
	private Iterable<String> uris;
	private boolean missingContentShouldBeMarkedAsUnavailable;
	
	public UriRefresher() {

	}
	
	public UriRefresher(Fetcher<Content> fetcher, ContentWriter contentStore) {
		this.fetcher = fetcher;
		this.contentStore = contentStore;
	}

	public void run() {
		String lastUri = "";
		
		for (String uri : uris) {
			update(uri);
			lastUri = uri;
		}
		log.info("Update complete, last uri checked was " + lastUri);
	}

	private void update(String uri) {
		
		Content latestContent = null;
		
		try {
			latestContent = fetcher.fetch(uri);
			if (latestContent == null) {
				throw new ContentNotFoundException(uri);
			}
			log.info("Retrieved data from URIplay for " + uri);
		} catch (Exception e) {
			log.warn(e);
			return;
		}

		try {
			createOrUpdateContent(latestContent, missingContentShouldBeMarkedAsUnavailable);
		} catch (Exception e) {
			log.warn(e);
			return;
		}
	
		log.info("Completed update of content in database for: " + uri);
	}
	
	 private void createOrUpdateContent(Content root, boolean markMissingItemsAsUnavailable) {
	        if (root instanceof Playlist) {
	        	contentStore.createOrUpdatePlaylist((Playlist) root, markMissingItemsAsUnavailable);
	        }
	        if (root instanceof Item) {
	        	contentStore.createOrUpdateItem((Item) root);
	        }
	    }
	
	@Required
	public void setFetcher(Fetcher<Content> fetcher) {
		this.fetcher = fetcher;
	}

	@Required
	public void setContentStore(ContentWriter contentStore) {
		this.contentStore = contentStore;
	}

	@Required
	public void setMissingContentShouldBeMarkedAsUnavailable(boolean missingContentShouldBeMarkedAsUnavailable) {
		this.missingContentShouldBeMarkedAsUnavailable = missingContentShouldBeMarkedAsUnavailable;
	}
	
	public void setUri(String uri) {
		setUris(Collections.singletonList(uri));
	}

	public void setUris(Iterable<String> uris) {
		this.uris = uris;
	}
}