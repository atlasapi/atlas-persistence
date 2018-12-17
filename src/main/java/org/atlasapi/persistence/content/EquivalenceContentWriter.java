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

package org.atlasapi.persistence.content;

import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Item;

/**
 * This interface was created to allow writing an empty equivalence set without having to change all other ContentWriter
 * implementations to include default behaviour for this new flag. Since very few ContentWriter implementations deal
 * with equivalence it made sense to add this separately and modify the few which do.
 */
public interface EquivalenceContentWriter extends ContentWriter {

	Item createOrUpdate(Item item, boolean writeEquivalencesIfEmpty);
	
	void createOrUpdate(Container container, boolean writeEquivalencesIfEmpty);

}
