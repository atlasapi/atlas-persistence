package org.atlasapi.persistence.media.entity;

import java.util.Set;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Item;
import org.atlasapi.persistence.ModelTranslator;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.mongodb.DBObject;

public class ContainerTranslator implements ModelTranslator<Container<?>> {
    
	private static final String CONTENTS_KEY = "contents";

	private final ContentTranslator contentTranslator;
	
	private final Function<DBObject, Item> embeddedItemFromDbo;
	private final Function<Item, DBObject> embeddedItemToDbo;
	
    public ContainerTranslator(boolean useIds) {
    	this.contentTranslator = new ContentTranslator(true);
    	final ItemTranslator embeddedItemTranslator = new ItemTranslator(false);
    	
    	this.embeddedItemFromDbo = new Function<DBObject, Item>() {
			@Override
			public Item apply(DBObject itemDbo) {
				return embeddedItemTranslator.fromDBObject(itemDbo, null);
			}
		};
		this.embeddedItemToDbo = new Function<Item, DBObject>() {
			@Override
			public DBObject apply(Item item) {
				return embeddedItemTranslator.toDBObject(null, item);
			}
		};
    }

    @Override
	@SuppressWarnings("unchecked")
    public Container<?> fromDBObject(DBObject dbObject, Container<?> entity) {
        if (entity == null) {
            entity = newModel(dbObject);
        }
        
        contentTranslator.fromDBObject(dbObject, entity);
        
        if (dbObject.containsField(CONTENTS_KEY)) {
        	Iterable<DBObject> contentDbos = (Iterable<DBObject>) dbObject.get(CONTENTS_KEY);
			
			((Container<Item>) entity).setContents(Iterables.transform(contentDbos, embeddedItemFromDbo));
        }
        
        return entity;
    }

	private Container<?> newModel(DBObject dbo) {
		String type = (String) dbo.get("type");
		if (type.equals(Brand.class.getSimpleName())) {
			return new Brand();
		}
		if (type.equals(Container.class.getSimpleName())) {
			return new Container<Item>();
		}
		throw new IllegalStateException();
	}

	@Override
	@SuppressWarnings("unchecked")
    public DBObject toDBObject(DBObject dbObject, Container<?> entity) {
        dbObject = contentTranslator.toDBObject(dbObject, entity);
        
        Set<String> lookup = Sets.newHashSet((Iterable<String>) dbObject.get(DescriptionTranslator.LOOKUP));
        
        for (Item item : entity.getContents()) {
        	lookup.addAll(DescriptionTranslator.lookupElemsFor(item));
        }
        dbObject.put(DescriptionTranslator.LOOKUP, lookup);
        
        dbObject.put("type", entity.getClass().getSimpleName());
        dbObject.put(CONTENTS_KEY, Iterables.transform(entity.getContents(), embeddedItemToDbo));
        return dbObject;
    }
}
