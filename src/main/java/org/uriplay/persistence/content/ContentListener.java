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

import java.util.Collection;

import org.uriplay.media.entity.Brand;
import org.uriplay.media.entity.Item;

public interface ContentListener {

	/**
	 * Type of change to the data. Options are:
	 * <ul>
	 * <li>BOOTSTRAP: Initial bootstrap of data</li>
	 * <li>CONTENT_UPDATE: Standard Content data update</li>
	 * </ul>
	 */
	public enum changeType {
		BOOTSTRAP,
		CONTENT_UPDATE;
	}
	
	void itemChanged(Collection<Item> item, changeType changeType);
	
	void brandChanged(Collection<Brand> brand, changeType changeType);
}
