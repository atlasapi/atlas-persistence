package org.uriplay.persistence;

/**
 * @author Robert Chatley (robert@metabroadcast.com)
 */
public interface ReadableBeanStore extends BeanStore {

	Iterable<Object> find(String query, String param);

}
