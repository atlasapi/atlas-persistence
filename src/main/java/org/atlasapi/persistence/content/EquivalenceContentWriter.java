package org.atlasapi.persistence.content;

import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;

import javax.annotation.Nullable;
import java.util.Set;

/**
 * This interface was created to allow writing an empty equivalence set without having to change all other ContentWriter
 * implementations to include default behaviour for this new flag. Since very few ContentWriter implementations deal
 * with equivalence it made sense to add this separately and modify the few which do.
 */
public interface EquivalenceContentWriter extends ContentWriter {

	Item createOrUpdate(Item item, @Nullable Set<Publisher> publishers, boolean writeEquivalencesIfEmpty);
	
	void createOrUpdate(Container container, @Nullable Set<Publisher> publishers, boolean writeEquivalencesIfEmpty);

}
