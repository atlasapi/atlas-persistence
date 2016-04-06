package org.atlasapi.persistence.media.entity;

import java.util.Set;

import org.atlasapi.media.entity.Award;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class AwardTranslator {

    public static final String OUTCOME = "outcome";
    public static final String TITLE = "title";
    public static final String DESCRIPTION = "description";
    public static final String YEAR = "year";

    public DBObject toDBObject(Award award) {
        DBObject dbo = new BasicDBObject();
        TranslatorUtils.from(dbo, OUTCOME, award.getOutcome());
        TranslatorUtils.from(dbo, TITLE, award.getTitle());
        TranslatorUtils.from(dbo, DESCRIPTION, award.getDescription());
        TranslatorUtils.from(dbo, YEAR, award.getYear());
        return dbo;
    }

    public Award fromDBObject(DBObject dbo) {
        Award award = new Award();
        award.setOutcome(TranslatorUtils.toString(dbo, OUTCOME));
        award.setTitle(TranslatorUtils.toString(dbo, TITLE));
        award.setDescription(TranslatorUtils.toString(dbo, DESCRIPTION));
        award.setYear(TranslatorUtils.toInteger(dbo, YEAR));
        return award;
    }

    public Set<Award> fromDBObjects(Iterable<DBObject> dbObjects) {
        return ImmutableSet.copyOf(Iterables.transform(dbObjects, TO_AWARDS));
    }

    public BasicDBList toDBList(Iterable<Award> awards) {
        BasicDBList list = new BasicDBList();
        list.addAll(ImmutableSet.copyOf(Iterables.transform(awards, TO_DB_OBJECT)));
        return list;
    }

    private Function<Award, DBObject> TO_DB_OBJECT = new Function<Award, DBObject>() {
        @Override
        public DBObject apply(Award award) {
            return toDBObject(award);
        }
    };

    private Function<DBObject, Award> TO_AWARDS = new Function<DBObject, Award>() {
        @Override
        public Award apply(DBObject dbObject) {
            return fromDBObject(dbObject);
        }
    };
}
