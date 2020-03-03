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
import org.atlasapi.persistence.lookup.entry.EquivRefs.EquivDirection;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.media.entity.AliasTranslator;
import org.atlasapi.persistence.media.entity.LookupRefTranslator;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.metabroadcast.common.persistence.mongo.MongoConstants.ID;
import static org.atlasapi.persistence.lookup.entry.EquivRefs.EquivDirection.BIDIRECTIONAL;
import static org.atlasapi.persistence.lookup.entry.EquivRefs.EquivDirection.OUTGOING;

public class LookupEntryTranslator {

    private static final String EXPLICIT = "explicit";
    private static final String DIRECT = "direct";
    private static final String BLACKLISTED = "blacklisted";
    private static final String EQUIVS = "equivs";
    private static final String LAST_UPDATED = "updated";
    private static final String EQUIV_LAST_UPDATED = "equivLastUpdated";
    private static final String FIRST_CREATED = "created";
    public static final String ACTIVELY_PUBLISHED = "activelyPublished";
    public static final String ALIASES = "aliases";
    public static final String IDS = "ids";
    public static final String OPAQUE_ID = "aid";
    public static final String SELF = "self";
    public static final String EQUIV_DIRECTION = "equivDirection";

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
        TranslatorUtils.fromDateTime(dbo, EQUIV_LAST_UPDATED, entry.equivUpdated());

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
        for (Map.Entry<LookupRef, EquivDirection> equivRef : equivRefs.getEquivRefs().entrySet()) {
            DBObject equivRefDbo = refToDbo.apply(equivRef.getKey());
            TranslatorUtils.from(equivRefDbo, EQUIV_DIRECTION, equivRef.getValue().toString());
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
        DateTime equivUpdated = TranslatorUtils.toDateTime(dbo, EQUIV_LAST_UPDATED);

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
        dbo.removeField(EQUIV_LAST_UPDATED);
        return dbo;
    }

    private ImmutableSet<LookupRef> translateRefs(LookupRef selfRef, DBObject dbo, String field) {
        return ImmutableSet.<LookupRef>builder()
                .add(selfRef)
                .addAll(Iterables.transform(TranslatorUtils.toDBObjectList(dbo, field), refFromDbo))
                .build();
    }

    private EquivRefs translateEquivRefs(@Nullable LookupRef selfRef, DBObject dbo, String field) {
        ImmutableMap.Builder<LookupRef, EquivDirection> equivRefs = ImmutableMap.builder();

        if (selfRef != null) {
            equivRefs.put(selfRef, BIDIRECTIONAL);
        }

        List<DBObject> equivRefDbos = TranslatorUtils.toDBObjectList(dbo, field);
        for (DBObject equivRefDbo : equivRefDbos) {
            LookupRef ref = refFromDbo.apply(equivRefDbo);

            if (ref == null || ref.equals(selfRef)) {
                continue;
            }

            String equivDirectionStr = TranslatorUtils.toString(equivRefDbo, EQUIV_DIRECTION);
            // If we don't know we assume it's an outgoing link so that we mirror the old behaviour which
            // would break an incoming link during equiv if it did not become an outgoing link.
            // If we treated this as a incoming or bidirectional link we would end up in a situation where bad
            // equiv is hard to break, since some sources like PA do not have equiv run on them.
            EquivDirection equivDirection = Strings.isNullOrEmpty(equivDirectionStr)
                    ? OUTGOING
                    : EquivRefs.EquivDirection.valueOf(equivDirectionStr);
            equivRefs.put(ref, equivDirection);
        }

        return EquivRefs.of(equivRefs.build());
    }

    private static final Function<DBObject, LookupRef> refFromDbo = input -> lookupRefTranslator.fromDBObject(input, null);
}
