package org.atlasapi.persistence.lookup.mongo;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import joptsimple.internal.Strings;
import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.persistence.lookup.entry.EquivRefs;
import org.atlasapi.persistence.lookup.entry.EquivRefs.Direction;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.media.entity.AliasTranslator;
import org.atlasapi.persistence.media.entity.LookupRefTranslator;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.metabroadcast.common.persistence.mongo.MongoConstants.ID;
import static org.atlasapi.persistence.lookup.entry.EquivRefs.Direction.BIDIRECTIONAL;

public class LookupEntryTranslator {

    private static final String EXPLICIT = "explicit";
    private static final String DIRECT = "direct";
    private static final String BLACKLISTED = "blacklisted";
    private static final String EQUIVS = "equivs";
    public static final String LAST_UPDATED = "updated";
    public static final String TRANSITIVES_UPDATED = "transitivesUpdated";
    public static final String FIRST_CREATED = "created";
    public static final String ACTIVELY_PUBLISHED = "activelyPublished";
    public static final String ALIASES = "aliases";
    public static final String IDS = "ids";
    public static final String OPAQUE_ID = "aid";
    public static final String SELF = "self";
    public static final String REF = "ref";
    public static final String DIRECTION = "direction";

    private final AliasTranslator aliasTranslator = new AliasTranslator();
    private static final LookupRefTranslator lookupRefTranslator = new LookupRefTranslator();
    
    public Function<LookupEntry, DBObject> TO_DBO = input -> toDbo(input);

    public Function<DBObject, LookupEntry> FROM_DBO = dbo -> fromDbo(dbo);
    
    public DBObject toDbo(LookupEntry entry) {
        BasicDBObject dbo = new BasicDBObject();
        
        TranslatorUtils.from(dbo, ID, entry.uri());
        TranslatorUtils.from(dbo, OPAQUE_ID, entry.id());
        TranslatorUtils.from(dbo, SELF, refToDbo.apply(entry.lookupRef()));
        
        Set<String> aliases = Sets.newHashSet(entry.uri());
        aliases.addAll(entry.aliasUrls());
        TranslatorUtils.fromSet(dbo, aliases, ALIASES);
        
        TranslatorUtils.from(dbo, IDS, aliasTranslator.toDBList(entry.aliases()));

        translateRefsIntoField(dbo, EQUIVS, entry.equivalents());
        translateEquivRefsIntoField(dbo, DIRECT, entry.directEquivalents());
        translateEquivRefsIntoField(dbo, EXPLICIT, entry.explicitEquivalents());
        translateEquivRefsIntoField(dbo, BLACKLISTED, entry.blacklistedEquivalents());

        TranslatorUtils.fromDateTime(dbo, FIRST_CREATED, entry.created());
        TranslatorUtils.fromDateTime(dbo, LAST_UPDATED, entry.updated());
        TranslatorUtils.fromDateTime(dbo, TRANSITIVES_UPDATED, entry.transitivesUpdated());

        if (!entry.activelyPublished()) {
            TranslatorUtils.from(dbo, ACTIVELY_PUBLISHED, entry.activelyPublished());
        }
        
        return dbo;
    }

    private void translateRefsIntoField(BasicDBObject dbo, String field, Set<LookupRef> refs) {
        BasicDBList refDbos = new BasicDBList();
        refDbos.addAll(ImmutableSet.copyOf(Iterables.transform(refs, refToDbo)));
        TranslatorUtils.from(dbo, field, refDbos);
    }

    private void translateEquivRefsIntoField(BasicDBObject dbo, String field, EquivRefs equivRefs) {
        BasicDBList equivRefDbos = new BasicDBList();
        for (Map.Entry<LookupRef, Direction> equivRef : equivRefs.getEquivRefsAsMap().entrySet()) {
            DBObject equivRefDbo = new BasicDBObject();
            TranslatorUtils.from(equivRefDbo, REF, refToDbo.apply(equivRef.getKey()));
            TranslatorUtils.from(equivRefDbo, DIRECTION, equivRef.getValue().toString());
            equivRefDbos.add(equivRefDbo);
        }
        TranslatorUtils.from(dbo, field, equivRefDbos);
    }
    
    private static Function<LookupRef, DBObject> refToDbo = input ->
            lookupRefTranslator.toDBObject(null, input);
    
    public LookupEntry fromDbo(DBObject dbo) {
        if(dbo == null) {
            return null;
        }
        
        String uri = TranslatorUtils.toString(dbo, ID);
        Long id = TranslatorUtils.toLong(dbo, OPAQUE_ID);
        
        Set<String> aliasUris = TranslatorUtils.toSet(dbo, ALIASES);
        aliasUris.add(uri);
        
        Set<Alias> aliases = aliasTranslator.fromDBObjects(TranslatorUtils.toDBObjectList(dbo, IDS));
        
        LookupRef self = lookupRefTranslator.fromDBObject(TranslatorUtils.toDBObject(dbo, SELF), null);
        Set<LookupRef> equivs = translateRefs(self, dbo, EQUIVS);
        EquivRefs directEquivalents = translateEquivRefs(self, dbo, DIRECT);
        EquivRefs explicitEquivalents = translateEquivRefs(self, dbo, EXPLICIT);
        EquivRefs blacklistedEquivalents = translateEquivRefs(null, dbo, BLACKLISTED); //Don't include self

        DateTime created = TranslatorUtils.toDateTime(dbo, FIRST_CREATED);
        DateTime updated = TranslatorUtils.toDateTime(dbo, LAST_UPDATED);
        DateTime equivUpdated = TranslatorUtils.toDateTime(dbo, TRANSITIVES_UPDATED);

        boolean activelyPublished = true;
        if (dbo.containsField(ACTIVELY_PUBLISHED)) {
            activelyPublished = TranslatorUtils.toBoolean(dbo, ACTIVELY_PUBLISHED);
        }
        return new LookupEntry(
                uri,
                id,
                self,
                aliasUris,
                aliases,
                directEquivalents,
                explicitEquivalents,
                blacklistedEquivalents,
                equivs,
                created,
                updated,
                equivUpdated,
                activelyPublished
        );
    }
    
    public DBObject removeFieldsForHash(DBObject dbo) {
        if (dbo == null) {
            return null;
        }
        dbo.removeField(LAST_UPDATED);
        dbo.removeField(FIRST_CREATED);
        dbo.removeField(TRANSITIVES_UPDATED);
        return dbo;
    }

    private ImmutableSet<LookupRef> translateRefs(LookupRef selfRef, DBObject dbo, String field) {
        return ImmutableSet.<LookupRef>builder()
                .add(selfRef)
                .addAll(Iterables.transform(TranslatorUtils.toDBObjectList(dbo, field), refFromDbo))
                .build();
    }

    private EquivRefs translateEquivRefs(@Nullable LookupRef selfRef, DBObject dbo, String field) {
        ImmutableMap.Builder<LookupRef, Direction> equivRefs = ImmutableMap.builder();

        if (selfRef != null) {
            equivRefs.put(selfRef, BIDIRECTIONAL);
        }

        List<DBObject> equivRefDbos = TranslatorUtils.toDBObjectList(dbo, field);
        for (DBObject equivRefDbo : equivRefDbos) {
            DBObject lookupRefDbo = TranslatorUtils.toDBObject(equivRefDbo, REF);
            LookupRef ref;
            if (lookupRefDbo != null) {
                ref = refFromDbo.apply(lookupRefDbo);
            } else {
                // For pre-existing lookup refs which are not nested
                ref = refFromDbo.apply(equivRefDbo);
            }

            if (ref == null || ref.equals(selfRef)) {
                continue;
            }

            String directionStr = TranslatorUtils.toString(equivRefDbo, DIRECTION);
            // If we don't know we assume it's a bidirectional link in order to keep our logic consistent.
            // If two different pieces of content which were equived only thought they either each
            // have an outgoing link to the other, or each have an incoming link from the other, this would actually
            // be a bidirectional link.
            Direction direction = Strings.isNullOrEmpty(directionStr)
                    ? BIDIRECTIONAL
                    : Direction.valueOf(directionStr);
            equivRefs.put(ref, direction);
        }

        return EquivRefs.of(equivRefs.build());
    }

    private static final Function<DBObject, LookupRef> refFromDbo = input -> lookupRefTranslator.fromDBObject(input, null);
}
