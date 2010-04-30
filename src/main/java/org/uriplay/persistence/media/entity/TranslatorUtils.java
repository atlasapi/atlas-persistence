package org.uriplay.persistence.media.entity;

import java.util.List;
import java.util.Set;

import org.joda.time.DateTime;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.mongodb.BasicDBList;
import com.mongodb.DBObject;

public class TranslatorUtils {
    public static String toString(Object objectId) {
        String value = null;
        if (objectId != null) {
            value = objectId.toString();
        }
        return value;
    }

    @SuppressWarnings("unchecked")
    public static Set<String> toSet(DBObject object, String name) {
        if (object.containsField(name)) {
            List<String> dbValues = (List<String>) object.get(name);
            return Sets.newHashSet(dbValues);
        }
        return Sets.newHashSet();
    }

    @SuppressWarnings("unchecked")
    public static List<String> toList(DBObject object, String name) {
        if (object.containsField(name)) {
            return (List<String>) object.get(name);
        }
        return Lists.newArrayList();
    }

    public static DateTime toDateTime(DBObject dbObject, String name) {
        if (dbObject.containsField(name)) {
            Long millis = (Long) dbObject.get(name);
            return new DateTime(millis);
        }
        return null;
    }

    public static void fromSet(DBObject dbObject, Set<String> set, String name) {
        if (!set.isEmpty()) {
            BasicDBList values = new BasicDBList();
            for (String value : set) {
                if (value != null) {
                    values.add(value);
                }
            }
            dbObject.put(name, values);
        }
    }

    public static void fromList(DBObject dbObject, List<String> list, String name) {
        if (!list.isEmpty()) {
            BasicDBList values = new BasicDBList();
            for (String value : list) {
                if (value != null) {
                    values.add(value);
                }
            }
            dbObject.put(name, values);
        }
    }

    public static void fromDateTime(DBObject dbObject, String name, DateTime dateTime) {
        if (dateTime != null) {
            Long millis = ((Number) dateTime.getMillis()).longValue();
            dbObject.put(name, millis);
        }
    }

    public static void from(DBObject dbObject, String name, Object value) {
        if (value != null) {
            dbObject.put(name, value);
        }
    }
}
