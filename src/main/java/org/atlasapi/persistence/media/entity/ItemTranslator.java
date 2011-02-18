package org.atlasapi.persistence.media.entity;

import java.util.List;
import java.util.Set;

import org.atlasapi.media.entity.CrewMember;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Version;
import org.atlasapi.persistence.ModelTranslator;

import com.google.common.collect.Sets;
import com.mongodb.BasicDBList;
import com.mongodb.DBObject;

public class ItemTranslator implements ModelTranslator<Item> {
    
	private final ContentTranslator contentTranslator;
    private final VersionTranslator versionTranslator = new VersionTranslator();
    private final CrewMemberTranslator crewMemberTranslator = new CrewMemberTranslator();
    
    public ItemTranslator(ContentTranslator contentTranslator) {
		this.contentTranslator = contentTranslator;
    }
    
    public ItemTranslator() {
    	this(new ContentTranslator(new DescriptionTranslator(true), new ClipTranslator(false)));
    }
    
    
    @SuppressWarnings("unchecked")
    @Override
    public Item fromDBObject(DBObject dbObject, Item entity) {
        if (entity == null) {
            entity = new Item();
        }
        
        contentTranslator.fromDBObject(dbObject, entity);
        
        entity.setIsLongForm((Boolean) dbObject.get("isLongForm"));
        List<DBObject> list = (List) dbObject.get("versions");
        if (list != null && ! list.isEmpty()) {
            Set<Version> versions = Sets.newHashSet();
            for (DBObject object: list) {
                Version version = versionTranslator.fromDBObject(object, null);
                versions.add(version);
            }
            entity.setVersions(versions);
        }
        
        list = (List) dbObject.get("people");
        if (list != null && ! list.isEmpty()) {
            for (DBObject dbPerson: list) {
                CrewMember crewMember = crewMemberTranslator.fromDBObject(dbPerson, null);
                if (crewMember != null) {
                    entity.addPerson(crewMember);
                }
            }
        }
        
        return entity;
    }

    @Override
    public DBObject toDBObject(DBObject dbObject, Item entity) {
        dbObject = contentTranslator.toDBObject(dbObject, entity);
        
        dbObject.put("isLongForm", entity.getIsLongForm());
        if (! entity.getVersions().isEmpty()) {
            BasicDBList list = new BasicDBList();
            for (Version version: entity.getVersions()) {
                list.add(versionTranslator.toDBObject(null, version));
            }
            dbObject.put("versions", list);
        }
        
        if (! entity.people().isEmpty()) {
    		    BasicDBList list = new BasicDBList();
            for (CrewMember person: entity.people()) {
                list.add(crewMemberTranslator.toDBObject(null, person));
            }
            dbObject.put("people", list);
    		}
        
        return dbObject;
    }
}
