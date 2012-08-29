package org.atlasapi.persistence.cassandra;

/**
 */
public class CassandraDisplayTitle extends CassandraObject {

    public final static String TITLE = "title";
    public final static String SUBTITLE = "subtitle";
    
    public CassandraDisplayTitle title(String title) {
        properties.put(TITLE, title);
        return this;
    }
    
    public CassandraDisplayTitle subtitle(String subtitle) {
        properties.put(SUBTITLE, subtitle);
        return this;
    }
}
