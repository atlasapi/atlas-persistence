package org.atlasapi.persistence.lookup.mongo;

import static com.metabroadcast.common.persistence.mongo.MongoConstants.ID;

import java.util.List;
import java.util.Set;

import org.atlasapi.media.common.Id;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentCategory;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.joda.time.DateTime;

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class LookupEntryTranslator {

    private static final String SELF = "self";
    private static final String DIRECT = "direct";
    private static final String EQUIVS = "equivs";
    private static final String PUBLISHER = "publisher";
    private static final String TYPE = "type";
    private static final String LAST_UPDATED = "updated";
    private static final String FIRST_CREATED = "created";
    public static final String ALIASES = "aliases";
    public static final String OPAQUE_ID = "aid";
    public Function<LookupEntry, DBObject> TO_DBO = new Function<LookupEntry, DBObject>() {

        @Override
        public DBObject apply(LookupEntry input) {
            return toDbo(input);
        }
    };
    public Function<DBObject, LookupEntry> FROM_DBO = new Function<DBObject, LookupEntry>() {

        @Override
        public LookupEntry apply(DBObject dbo) {
            try {
                return fromDbo(dbo);
            } catch (Exception ex) {
                return null;
            }
        }
    };

    public DBObject toDbo(LookupEntry entry) {
        BasicDBObject dbo = new BasicDBObject();

        TranslatorUtils.from(dbo, ID, entry.uri());
        TranslatorUtils.from(dbo, OPAQUE_ID, entry.id().longValue());
        TranslatorUtils.from(dbo, SELF, equivalentToDbo.apply(entry.lookupRef()));

        Set<String> aliases = Sets.newHashSet(entry.uri());
        aliases.addAll(entry.aliases());
        TranslatorUtils.fromSet(dbo, aliases, ALIASES);

        BasicDBList directEquivDbos = new BasicDBList();
        directEquivDbos.addAll(ImmutableSet.copyOf(Iterables.transform(entry.directEquivalents(), equivalentToDbo)));
        TranslatorUtils.from(dbo, DIRECT, directEquivDbos);

        BasicDBList equivDbos = new BasicDBList();
        equivDbos.addAll(ImmutableSet.copyOf(Iterables.transform(entry.equivalents(), equivalentToDbo)));
        TranslatorUtils.from(dbo, EQUIVS, equivDbos);

        BasicDBList explicitDbos = new BasicDBList();
        explicitDbos.addAll(ImmutableSet.copyOf(Iterables.transform(entry.explicitEquivalents(), equivalentToDbo)));
        TranslatorUtils.from(dbo, "explicit", explicitDbos);

        TranslatorUtils.fromDateTime(dbo, FIRST_CREATED, entry.created());
        TranslatorUtils.fromDateTime(dbo, LAST_UPDATED, entry.updated());

        return dbo;
    }
    private static Function<LookupRef, DBObject> equivalentToDbo = new Function<LookupRef, DBObject>() {

        @Override
        public DBObject apply(LookupRef input) {
            BasicDBObject dbo = new BasicDBObject();

            TranslatorUtils.from(dbo, OPAQUE_ID, input.id().longValue());
            TranslatorUtils.from(dbo, PUBLISHER, input.publisher().key());
            TranslatorUtils.from(dbo, TYPE, input.category().toString());

            return dbo;
        }
    };

    public LookupEntry fromDbo(DBObject dbo) {
        if (dbo == null) {
            return null;
        }

        String uri = TranslatorUtils.toString(dbo, ID);
        Long lid = TranslatorUtils.toLong(dbo, OPAQUE_ID);
        Id id = lid != null ? Id.valueOf(lid) : null;

        Set<String> aliases = TranslatorUtils.toSet(dbo, ALIASES);
        aliases.add(uri);

        LookupRef self = equivalentFromDbo.apply(TranslatorUtils.toDBObject(dbo, SELF));
        Set<LookupRef> equivs = toLookupRefs(TranslatorUtils.toDBObjectList(dbo, EQUIVS));
        DateTime created = TranslatorUtils.toDateTime(dbo, FIRST_CREATED);
        DateTime updated = TranslatorUtils.toDateTime(dbo, LAST_UPDATED);

        Set<LookupRef> directEquivalents = toLookupRefs(TranslatorUtils.toDBObjectList(dbo, DIRECT));
        Set<LookupRef> explicitEquivalents = toLookupRefs(TranslatorUtils.toDBObjectList(dbo, "explicit"));

        return new LookupEntry(uri, id, self, aliases, directEquivalents, explicitEquivalents, equivs, created, updated);
    }
    private ImmutableSet<LookupRef> toLookupRefs(List<DBObject> refList) {
        return ImmutableSet.copyOf(Iterables.filter(Iterables.transform(refList, equivalentFromDbo), Predicates.notNull()));
    }
    private static final Function<DBObject, LookupRef> equivalentFromDbo = new Function<DBObject, LookupRef>() {

        @Override
        public LookupRef apply(DBObject input) {
            Long aid = TranslatorUtils.toLong(input, OPAQUE_ID);
            if (aid == null) {
                return null;
            }
            Id id = Id.valueOf(aid);
            String uri = TranslatorUtils.toString(input, ID);
            Publisher publisher = Publisher.fromKey(TranslatorUtils.toString(input, PUBLISHER)).requireValue();
            String type = TranslatorUtils.toString(input, TYPE);
            return new LookupRef(id, publisher, ContentCategory.valueOf(type));
        }
    };
}
