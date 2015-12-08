package org.atlasapi.persistence.content.mongo;


public interface TopLevelItemsEntry {

    public Iterable<String> getItemUrisForEventIds(Iterable<Long> eventIds);

}
