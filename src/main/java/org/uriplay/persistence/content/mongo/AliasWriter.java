package org.uriplay.persistence.content.mongo;

import java.util.Set;

public interface AliasWriter {

    void addAliases(String uri, Set<String> aliases);

}
