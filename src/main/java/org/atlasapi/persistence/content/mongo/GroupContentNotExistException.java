package org.atlasapi.persistence.content.mongo;

import com.google.common.collect.Iterables;

public class GroupContentNotExistException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public GroupContentNotExistException(String groupUri, Iterable<String> expectedContentUris) {
        super("Not all content exists in the database for group: " + groupUri + " expecting content uris of: " + Iterables.toString(expectedContentUris));
    }
}
