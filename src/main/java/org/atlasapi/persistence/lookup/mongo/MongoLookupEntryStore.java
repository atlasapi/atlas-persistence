package org.atlasapi.persistence.lookup.mongo;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.persistence.mongo.DatabasedMongoClient;
import com.metabroadcast.common.persistence.mongo.MongoBuilders;
import com.metabroadcast.common.persistence.mongo.MongoConstants;
import com.metabroadcast.common.persistence.mongo.MongoQueryBuilder;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.metabroadcast.common.query.Selection;
import com.metabroadcast.common.stream.MoreCollectors;
import com.mongodb.DBObject;
import com.mongodb.MongoClientException;
import com.mongodb.ReadPreference;
import com.mongodb.TransactionOptions;
import com.mongodb.client.ClientSession;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.ReplaceOptions;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.Transaction;
import org.atlasapi.persistence.audit.PersistenceAuditLog;
import org.atlasapi.persistence.content.ContentCategory;
import org.atlasapi.persistence.content.listing.ContentListingProgress;
import org.atlasapi.persistence.lookup.NewLookupWriter;
import org.atlasapi.persistence.lookup.entry.EquivRefs;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.atlasapi.persistence.media.entity.IdentifiedTranslator;
import org.bson.Document;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.metabroadcast.common.persistence.mongo.MongoBuilders.select;
import static com.metabroadcast.common.persistence.mongo.MongoBuilders.sort;
import static com.metabroadcast.common.persistence.mongo.MongoBuilders.where;
import static com.metabroadcast.common.persistence.mongo.MongoConstants.ID;
import static com.metabroadcast.common.persistence.mongo.MongoConstants.IN;
import static org.atlasapi.persistence.lookup.entry.EquivRefs.Direction.BIDIRECTIONAL;
import static org.atlasapi.persistence.lookup.entry.LookupEntry.lookupEntryFrom;
import static org.atlasapi.persistence.lookup.mongo.LookupEntryTranslator.ACTIVELY_PUBLISHED;
import static org.atlasapi.persistence.lookup.mongo.LookupEntryTranslator.ALIASES;
import static org.atlasapi.persistence.lookup.mongo.LookupEntryTranslator.IDS;
import static org.atlasapi.persistence.lookup.mongo.LookupEntryTranslator.LAST_UPDATED;
import static org.atlasapi.persistence.lookup.mongo.LookupEntryTranslator.OPAQUE_ID;
import static org.atlasapi.persistence.lookup.mongo.LookupEntryTranslator.SELF;
import static org.atlasapi.persistence.lookup.mongo.LookupEntryTranslator.TRANSITIVES_UPDATED;
import static org.atlasapi.persistence.media.entity.AliasTranslator.NAMESPACE;
import static org.atlasapi.persistence.media.entity.AliasTranslator.VALUE;

public class MongoLookupEntryStore implements LookupEntryStore, NewLookupWriter {

    private static final String PUBLISHER = SELF + "." + IdentifiedTranslator.PUBLISHER;
    private static final Pattern ANYTHING = Pattern.compile("^.*");

    private static final Function<ContentCategory, String> CONTENT_CATEGORY_TO_NAME =
            new Function<ContentCategory, String>() {
        @Nullable @Override public String apply(@Nullable ContentCategory input) {
            return input != null ? input.name() : null;
        }
    };

    private final Logger log;
    private final MongoCollection<DBObject> lookupPrimaryRead;
    private final MongoCollection<DBObject> lookupSpecifiedRead;
    private final DatabasedMongoClient mongo;
    private final LookupEntryTranslator translator;
    private final LookupEntryHasher lookupEntryHasher;
    private final PersistenceAuditLog persistenceAuditLog;

    public MongoLookupEntryStore(
            DatabasedMongoClient mongo,
            String lookupCollectionName,
            PersistenceAuditLog persistenceAuditLog,
            ReadPreference readPreference
    ) {
        this(
                mongo,
                lookupCollectionName,
                readPreference,
                persistenceAuditLog,
                LoggerFactory.getLogger(MongoLookupEntryStore.class)
        );
    }

    public MongoLookupEntryStore(
            DatabasedMongoClient mongo,
            String lookupCollectionName,
            ReadPreference readPreference,
            PersistenceAuditLog persistenceAuditLog,
            Logger log
    ) {
        this.mongo = checkNotNull(mongo);
        MongoCollection<DBObject> lookup = mongo.collection(lookupCollectionName, DBObject.class);
        this.lookupPrimaryRead = lookup.withReadPreference(ReadPreference.primary());
        this.lookupSpecifiedRead = lookup.withReadPreference(readPreference);
        this.persistenceAuditLog = checkNotNull(persistenceAuditLog);
        this.translator = new LookupEntryTranslator();
        this.lookupEntryHasher = new LookupEntryHasher(translator);
        this.log = checkNotNull(log);
    }

    @Override
    public Transaction startTransaction() {
        try {
            ClientSession session = mongo.getMongoClient().startSession();
            TransactionOptions transactionOptions = TransactionOptions.builder()
                    // Multi-document transactions require reading from primary
                    // we're currently only using transactions for such cases so this is to enforce it is used correctly
                    .readPreference(ReadPreference.primary())
                    .build();
            session.startTransaction(transactionOptions);
            return Transaction.of(session);
        } catch (MongoClientException e) {
            log.error(
                    "Unable to start a session (Mongo version might be too old, or the instance is not" +
                            " in a replica set), continuing without using a session",
                    e
            );
            return Transaction.none();
        }
    }

    @Override
    public void store(LookupEntry entry) {
        store(Transaction.none(), entry);
    }

    @Override
    public void store(Transaction transaction, LookupEntry entry) {
        Document queryDocument = new Document(MongoConstants.ID, entry.uri());
        LookupEntry existing;
        if (transaction.getSession() == null) {
            existing = lookupPrimaryRead.find(queryDocument).map(translator::fromDbo).first();
        } else {
            existing = lookupPrimaryRead.find(transaction.getSession(), queryDocument).map(translator::fromDbo).first();
        }

        store(transaction, entry, existing);
    }

    private void store(Transaction transaction, LookupEntry newEntry, @Nullable LookupEntry existingEntry) {
        if (existingEntry != null
                && lookupEntryHasher.writeHashFor(newEntry) == lookupEntryHasher.writeHashFor(existingEntry)) {
            log.debug("Hash code not changed for URI {}; skipping write", newEntry.uri());
            persistenceAuditLog.logNoWrite(newEntry);
            return;
        }
        log.debug("New entry or hash code changed for URI {}; writing", newEntry.uri());
        persistenceAuditLog.logWrite(newEntry);

        Document queryDocument = MongoBuilders.where().idEquals(newEntry.uri()).buildAsDocument();
        ReplaceOptions replaceOptions = new ReplaceOptions();
        replaceOptions.upsert(true);

        if (transaction.getSession() == null) {
            lookupPrimaryRead.replaceOne(
                    queryDocument,
                    translator.toDbo(newEntry),
                    replaceOptions
            );
        } else {
            lookupPrimaryRead.replaceOne(
                    transaction.getSession(),
                    queryDocument,
                    translator.toDbo(newEntry),
                    replaceOptions
            );
        }
    }


    @Override
    public Iterable<LookupEntry> entriesForCanonicalUris(Iterable<String> uris) {
        return entriesForCanonicalUris(Transaction.none(), uris);
    }

    @Override
    public Iterable<LookupEntry> entriesForCanonicalUris(Transaction transaction, Iterable<String> uris) {
        Document queryDocument = where().idIn(uris).buildAsDocument();
        FindIterable<DBObject> found = transaction.getSession() == null
                ? lookupSpecifiedRead.find(queryDocument)
                : lookupSpecifiedRead.find(transaction.getSession(), queryDocument);
        return found.map(translator::fromDbo);
    }

    @Override
    public Iterable<LookupEntry> entriesForIds(Iterable<Long> ids) {
        return entriesForIds(Transaction.none(), ids);
    }

    @Override
    public Iterable<LookupEntry> entriesForIds(Transaction transaction, Iterable<Long> ids) {
        Document queryDocument = new Document(OPAQUE_ID, new Document(IN, ids));
        FindIterable<DBObject> found = transaction.getSession() == null
                ? lookupSpecifiedRead.find(queryDocument)
                : lookupSpecifiedRead.find(transaction.getSession(), queryDocument);

        return found.map(translator::fromDbo);
    }

    @Override
    public void ensureLookup(Content content) {
        LookupEntry newEntry = lookupEntryFrom(content);
        // Since most content will already have a lookup entry we read first to avoid locking the database
        LookupEntry existing = lookupPrimaryRead.find(new Document(MongoConstants.ID, content.getCanonicalUri()))
                .map(translator::fromDbo)
                .first();

        if (existing == null) {
            store(Transaction.none(), newEntry, null);
        } else if(!newEntry.lookupRef().category().equals(existing.lookupRef().category())) {
            updateEntry(content, newEntry, existing);
        } else if (!newEntry.aliasUrls().equals(existing.aliasUrls())
                || !newEntry.aliases().equals(existing.aliases())
                || newEntry.activelyPublished() != existing.activelyPublished()) {
            store(Transaction.none(), merge(content, newEntry, existing), existing);
        }
    }

    private void updateEntry(Content content, LookupEntry newEntry, LookupEntry existing) {
        LookupEntry merged = merge(content, newEntry, existing);
        LookupRef ref = merged.lookupRef();

        store(Transaction.none(), merged, existing);

        Set<String> transitiveUris = merged.equivalents().stream()
                .filter(equivRef -> !equivRef.equals(ref))
                .map(LookupRef::uri)
                .collect(MoreCollectors.toImmutableSet());

        // Update any instances of the ref from the entries equived to it
        for (LookupEntry entry : entriesForCanonicalUris(transitiveUris)) {
            EquivRefs.Direction directEquivLink = entry.directEquivalents().getLink(ref);
            EquivRefs.Direction explicitEquivLink = entry.explicitEquivalents().getLink(ref);
            EquivRefs.Direction blacklistedEquivLink = entry.blacklistedEquivalents().getLink(ref);
            if(directEquivLink != null) {
                entry = entry.copyWithDirectEquivalents(entry.directEquivalents().copyWithLink(ref, directEquivLink));
            }
            if(explicitEquivLink != null) {
                entry = entry.copyWithExplicitEquivalents(entry.explicitEquivalents().copyWithLink(ref, explicitEquivLink));
            }
            if(blacklistedEquivLink != null) {
                entry = entry.copyWithBlacklistedEquivalents(entry.blacklistedEquivalents().copyWithLink(ref, blacklistedEquivLink));
            }
            Set<LookupRef> newEquivs = ImmutableSet.<LookupRef>builder()
                    .add(ref)
                    .addAll(existing.equivalents())
                    .build();
            entry = entry.copyWithEquivalents(newEquivs);
            store(Transaction.none(), entry, existing);
        }
    }

    private LookupEntry merge(Content content, LookupEntry newEntry, LookupEntry existing) {
        LookupRef ref = LookupRef.from(content);

        // We copy the equiv refs to update the ref's content category if needed

        Set<LookupRef> transitiveEquivs = ImmutableSet.<LookupRef>builder()
                .add(ref)
                .addAll(existing.equivalents())
                .build();

        LookupEntry merged = new LookupEntry(
                newEntry.uri(),
                existing.id(),
                ref,
                newEntry.aliasUrls(),
                newEntry.aliases(),
                existing.directEquivalents().copyWithLink(ref, BIDIRECTIONAL),
                existing.explicitEquivalents().copyWithLink(ref, BIDIRECTIONAL),
                existing.blacklistedEquivalents(),
                transitiveEquivs,
                existing.created(),
                newEntry.updated(),
                newEntry.transitivesUpdated(),
                newEntry.activelyPublished()
        );
        return merged;
    }

    @Override
    public Iterable<LookupEntry> entriesForIdentifiers(Iterable<String> identifiers, boolean useAliases) {
        return find(identifiers).map(translator::fromDbo);
    }

    private FindIterable<DBObject> find(Iterable<String> identifiers) {
        return lookupSpecifiedRead.find(
                where().fieldIn(ALIASES, identifiers).buildAsDocument()
        );
    }

    @Override
    public Iterable<LookupEntry> entriesForAliases(Optional<String> namespace, Iterable<String> values) {
        return entriesForAliases(namespace, values, null, true);
    }

    @Override
    public Iterable<LookupEntry> entriesForAliases(
            Optional<String> namespace,
            Iterable<String> values,
            boolean includeUnpublishedEntries
    ) {
        return find(namespace, values, null, includeUnpublishedEntries).map(translator::fromDbo);
    }

    @Override
    public Iterable<LookupEntry> entriesForAliases(
            Optional<String> namespace,
            Iterable<String> values,
            @Nullable Iterable<Publisher> publishers,
            boolean includeUnpublishedEntries
    ) {
        return find(namespace, values, publishers, includeUnpublishedEntries).map(translator::fromDbo);
    }

    @Override
    public Map<String, Long> idsForCanonicalUris(Iterable<String> uris) {
        Builder<String, Long> results = ImmutableMap.builder();
        Iterable<DBObject> cursor = lookupSpecifiedRead.find(where().idIn(uris).buildAsDocument())
                .projection(select().field(OPAQUE_ID).field(ID).buildAsDocument());
        for (DBObject dbo : cursor) {
            Long id = TranslatorUtils.toLong(dbo, OPAQUE_ID);
            if (id != null) {
                results.put(TranslatorUtils.toString(dbo, ID), id);
            }
        }
        return results.build();
    }

    private FindIterable<DBObject> find(
            Optional<String> namespace,
            Iterable<String> values,
            @Nullable Iterable<Publisher> publishers,
            boolean includeUnpublishedEntries
    ) {
        MongoQueryBuilder query = namespace.isPresent()
                ? where().elemMatch(IDS, where().fieldEquals(NAMESPACE, namespace.get()).fieldIn(VALUE, values))
                : where().elemMatch(IDS, where().fieldEquals(NAMESPACE, ANYTHING).fieldIn(VALUE, values));
        if (!includeUnpublishedEntries) {
            // Not actively published content will have this value set to false
            // Actively published content will either have this value be true or null
            query.fieldNotEqualTo(ACTIVELY_PUBLISHED, false);
        }
        if (publishers != null) {
            query.fieldIn(PUBLISHER, Iterables.transform(publishers, Publisher.TO_KEY));
        }
        return lookupSpecifiedRead.find(query.buildAsDocument());
    }

    @Override
    public Iterable<LookupEntry> entriesForPublishers(Iterable<Publisher> publishers,
            @Nullable Selection selection) {
        FindIterable<DBObject> find = lookupSpecifiedRead.find(
                where()
                        .fieldIn(PUBLISHER, Iterables.transform(publishers, Publisher.TO_KEY))
                        // Not actively published content will have this value set to false
                        // Actively published content will either have this value be true or null
                        .fieldNotEqualTo(ACTIVELY_PUBLISHED, false)
                        .buildAsDocument()
        ).sort(sort().ascending(OPAQUE_ID).buildAsDocument());

        if (selection != null) {
            find.skip(selection.getOffset());
            find.limit(selection.getLimit());
        }

        return find.map(translator::fromDbo);
    }

    @Override
    public Iterable<LookupEntry> allEntriesForPublishers(Iterable<Publisher> publishers,
            ContentListingProgress progress) {
        FindIterable<DBObject> cursor = cursorForPublishers(publishers, progress);
        return cursor.map(translator::fromDbo);
    }

    public Iterable<LookupEntry> all() {
        return lookupSpecifiedRead.find().map(translator::fromDbo);
    }

    private FindIterable<DBObject> cursorForPublishers(Iterable<Publisher> publishers,
            ContentListingProgress progress) {
        MongoQueryBuilder queryBuilder = where()
                .fieldIn(PUBLISHER, Iterables.transform(publishers, Publisher.TO_KEY));

        if (!progress.equals(ContentListingProgress.START)) {
            limitQueryByProgress(progress, queryBuilder);
        }

        return lookupSpecifiedRead.find(queryBuilder.buildAsDocument())
                .sort(sort().ascending(OPAQUE_ID).buildAsDocument());
    }

    private void limitQueryByProgress(ContentListingProgress progress,
            MongoQueryBuilder queryBuilder) {
        Iterable<LookupEntry> progressedToEntry = entriesForCanonicalUris(
                ImmutableList.of(progress.getUri())
        );

        if (Iterables.isEmpty(progressedToEntry)) {
            return;
        }

        LookupEntry entry = Iterables.getOnlyElement(progressedToEntry);
        queryBuilder.fieldGreaterThan(OPAQUE_ID, entry.id());
    }

    @Override
    public Iterable<LookupEntry> updatedSince(Publisher publisher, DateTime dateTime) {
        Document query = where()
                .fieldAfter(LAST_UPDATED, dateTime)
                .fieldEquals(PUBLISHER, publisher.key())
                .fieldNotEqualTo(ACTIVELY_PUBLISHED, false)
                .buildAsDocument();

        return StreamSupport.stream(lookupSpecifiedRead.find(query).map(translator::fromDbo).spliterator(), false)
                .collect(Collectors.toList());
    }

    @Override
    public Iterable<LookupEntry> equivUpdatedSince(Publisher publisher, DateTime dateTime) {
        Document query = where()
                .fieldAfter(TRANSITIVES_UPDATED, dateTime)
                .fieldEquals(PUBLISHER, publisher.key())
                .fieldNotEqualTo(ACTIVELY_PUBLISHED, false)
                .buildAsDocument();

        return lookupSpecifiedRead.find(query).map(translator::fromDbo);
    }

}
