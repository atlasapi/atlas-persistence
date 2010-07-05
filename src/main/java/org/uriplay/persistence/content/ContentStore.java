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

import org.uriplay.media.entity.Content;
import org.uriplay.media.entity.Item;
import org.uriplay.media.entity.Playlist;

import com.metabroadcast.common.query.Selection;

/**
 * Simple interface to the store of available content.
 *
 * @author Robert Chatley (robert@metabroadcast.com)
 * @author John Ayres (john@metabroadcast.com)
 */
public interface ContentStore {

	Content findByUri(String uri);

	List<Item> listAllItems(Selection selection);
	
	List<Playlist> listAllPlaylists(Selection selection);
}
