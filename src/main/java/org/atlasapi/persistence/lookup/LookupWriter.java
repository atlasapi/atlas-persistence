package org.atlasapi.persistence.lookup;

import java.util.Set;

import org.atlasapi.equiv.ContentRef;
import org.atlasapi.media.entity.Publisher;

/**
 * <p>
 * Responsible recording equivalences between resources for later resolution as
 * an equivalence set.
 * </p>
 * 
 * <p>
 * A subject is recorded as being <i>directly</i> equivalent to set of other
 * resources which become neighbours of the subject in its equivalence graph.
 * Transitively, neighbours of the new neighbours join the equivalence graph if
 * not already present.
 * </p>
 * 
 * <p>
 * {@link org.atlasapi.persistence.lookup.entry.LookupEntry LookupEntry}s for
 * each affected resource in the equivalence set should be recorded in the
 * {@link org.atlasapi.persistence.lookup.entry.LookupEntryStore LookupEntryStore}.
 * </p>
 * 
 * @see org.atlasapi.persistence.lookup.entry.LookupEntry LookupEntry
 * @see org.atlasapi.persistence.lookup.entry.LookupEntryStore LookupEntryStore
 * @see ContentRef
 * @see Publisher
 */
public interface LookupWriter {

    /**
     * <p>Record <code>subject</code> as <i>directly</i> equivalent to each
     * <code>equivalents</code> <strong>only</strong>, i.e. creates an edge in
     * the subject's equivalence graph to each equivalent and removes edges to
     * nodes not in <code>equivalents</code>.</p>
     * 
     * <p>Only edges to nodes from <code>publishers</code> are affected. So, given
     * <strong>A</strong> from publisher <code>X</code>:
     * <ul>
     * <li>if <strong>A</strong> is in <code>equivalents</code> but
     * <code>publishers</code> doesn't contain <strong>X</strong> then an edge
     * will not be created to <strong>A</strong>.</li>
     * <li>if <strong>A</strong> was previously recorded as equivalent to the
     * subject but is no longer in <code>equivalents</code> and
     * <strong>X</strong> is in <code>publishers</code> then the edge to
     * <strong>A</strong> will be removed.</li>
     * <li>if <strong>A</strong> was previously recorded as equivalent to the
     * subject but is no longer in <code>equivalents</code> and
     * <strong>X</strong> is <strong>not</strong> in <code>publishers</code> then the edge to
     * <strong>A</strong> will remain.</li>
     * </ul></p>
     * 
     * @param subject - reference from which edges are being created. 
     * @param equivalents - references to which edges are being created.
     * @param publishers - sources of references which will have edges affected. 
     */
    void writeLookup(ContentRef subject, Iterable<ContentRef> equivalents, Set<Publisher> publishers);

}
