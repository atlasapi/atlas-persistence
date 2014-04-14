package org.atlasapi.persistence.content;

import java.util.Set;

import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Publisher;

/**
 * Remove all traces of content from a {@link Publisher} from the database
 * 
 * @author tom
 *
 */
public interface ContentPurger {

    /**
     * Removes all content for the specified publisher. If specified,
     * equivalences to publishers are maintained in the remote item
     * in the form of aliases. The method
     * {@link #restoreEquivalences} can then be used to restore 
     * the equivalences after a re-ingest is performed.
     * 
     * @param publisher     Publisher whose content should be removed
     * @param equivalencesToRetainAsAliases Publishers whose equivalences should
     *                                      be maintained by storing as aliases on
     *                                      those pieces of content
     */
    void purge(Publisher publisher, Set<Publisher> equivalencesToRetainAsAliases);

    /**
     * Restore equivalences previously saved in the target {@link Content}
     * as aliases
     * 
     * @param publisher Publisher to scan for equivalences to restore.
     */
    void restoreEquivalences(Publisher publisher);
}
