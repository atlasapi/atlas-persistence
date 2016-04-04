package org.atlasapi.persistence.media.entity;

import java.util.List;
import java.util.Set;

import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Ordering;
import org.atlasapi.media.entity.Certificate;
import org.atlasapi.media.entity.Clip;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.ContentGroupRef;
import org.atlasapi.media.entity.CrewMember;
import org.atlasapi.media.entity.EventRef;
import org.atlasapi.media.entity.KeyPhrase;
import org.atlasapi.media.entity.TopicRef;
import org.atlasapi.media.entity.Version;
import org.atlasapi.persistence.ModelTranslator;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.intl.Countries;
import com.metabroadcast.common.persistence.mongo.MongoConstants;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import javax.annotation.Nullable;

import static com.google.common.base.Preconditions.checkNotNull;

public class ContentTranslator implements ModelTranslator<Content> {

    public static final String PEOPLE = "people";
    public static String CLIPS_KEY = "clips";
    public static String TOPICS_KEY = "topics";
    private static final String PHRASES_KEY = "phrases";
    private static String CONTENT_GROUP_KEY = "contentGroups";
    private static final String CERTIFICATES_KEY = "certificates";
    private static final String LANGUAGES_KEY = "languages";
    private static final String YEAR_KEY = "year";
    private static final String GENERIC_DESCRIPTION_KEY = "genericDescription";
    private static final String SIMILAR_CONTENT_KEY = "similar";
    private static final String EVENTS_KEY = "events";
    private static final String EDITORIAL_PRIORITY_KEY = "editorialPriority";
    private static final String VERSIONS_KEY = "versions";
    private static final String AWARDS = "awards";

    private final ClipTranslator clipTranslator;
    private final KeyPhraseTranslator keyPhraseTranslator;
    private final DescribedTranslator describedTranslator;
    private final TopicRefTranslator contentTopicTranslator;
    private final ContentGroupRefTranslator contentGroupRefTranslator;
    private final CrewMemberTranslator crewMemberTranslator;
    private final SimilarContentRefTranslator similarContentRefTranslator;
    private final VersionTranslator versionTranslator;
    private final EventRefTranslator eventRefTranslator;
    private final AwardTranslator awardTranslator;

    public ContentTranslator(NumberToShortStringCodec idCodec) {
        this(new DescribedTranslator(new IdentifiedTranslator(), new ImageTranslator()), new ClipTranslator(idCodec), new VersionTranslator(idCodec));
    }

    //TODO: why not use collaborators interface here? ModelTranslator<Described> etc...
    public ContentTranslator(DescribedTranslator describedTranslator, ClipTranslator clipTranslator, VersionTranslator versionTranslator) {
        this.describedTranslator = checkNotNull(describedTranslator);
        this.clipTranslator = checkNotNull(clipTranslator);
        this.keyPhraseTranslator = new KeyPhraseTranslator();
        this.contentTopicTranslator = new TopicRefTranslator();
        this.contentGroupRefTranslator = new ContentGroupRefTranslator();
        this.crewMemberTranslator = new CrewMemberTranslator();
        this.similarContentRefTranslator = new SimilarContentRefTranslator();
        this.versionTranslator = checkNotNull(versionTranslator);
        this.eventRefTranslator = new EventRefTranslator();
        this.awardTranslator = new AwardTranslator();

    }

    @Override
    public Content fromDBObject(DBObject dbObject, Content entity) {
        describedTranslator.fromDBObject(dbObject, entity);

        decodeClips(dbObject, entity);
        decodeKeyPhrases(dbObject, entity);
        decodeTopics(dbObject, entity);
        decodeContentGroups(dbObject, entity);
        decodeLanguages(dbObject, entity);
        decodeEvents(dbObject, entity);
        decodeCertificates(dbObject, entity);
        entity.setYear(TranslatorUtils.toInteger(dbObject, YEAR_KEY));
        entity.setGenericDescription(TranslatorUtils.toBoolean(dbObject, GENERIC_DESCRIPTION_KEY));
        entity.setSimilarContent(similarContentRefTranslator.fromDBObjects(TranslatorUtils.toDBObjectList(dbObject, SIMILAR_CONTENT_KEY)));
        entity.setEditorialPriority(TranslatorUtils.toInteger(dbObject, EDITORIAL_PRIORITY_KEY));

        List<DBObject> list = TranslatorUtils.toDBObjectList(dbObject, PEOPLE);
        if (list != null && ! list.isEmpty()) {
            for (DBObject dbPerson: list) {
                CrewMember crewMember = crewMemberTranslator.fromDBObject(dbPerson, null);
                if (crewMember != null) {
                    entity.addPerson(crewMember);
                }
            }
        }

        List<DBObject> versionList = TranslatorUtils.toDBObjectList(dbObject, VERSIONS_KEY);
        if (versionList != null && ! versionList.isEmpty()) {
            Set<Version> versions = Sets.newHashSet();
            for (DBObject versionDbo: versionList) {
                if (versionDbo == null) {
                    throw new IllegalStateException("Cannot read item stored with null version: " + entity.getCanonicalUri());
                }
                Version version = versionTranslator.fromDBObject(versionDbo, null);
                versions.add(version);
            }
            entity.setVersions(versions);
        }
        if(dbObject.containsField(AWARDS)) {
            entity.setAwards(awardTranslator.fromDBObjects(
                    TranslatorUtils.toDBObjectList(dbObject, AWARDS)));
        }
        return entity;
    }

    protected void decodeLanguages(DBObject dbObject, Content entity) {
        if (dbObject.containsField(LANGUAGES_KEY)) {
            entity.setLanguages(TranslatorUtils.toSet(dbObject, LANGUAGES_KEY));
        }
    }
    
    protected void decodeCertificates(DBObject dbObject, Content entity) {
        if (dbObject.containsField(CERTIFICATES_KEY)) {
            List<DBObject> dbos = TranslatorUtils.toDBObjectList(dbObject, CERTIFICATES_KEY);
            entity.setCertificates(Iterables.transform(dbos, certificateFromDbo));
        }
    }
    
    private final Function<DBObject, Certificate> certificateFromDbo = new Function<DBObject, Certificate>() {
        @Override
        public Certificate apply(DBObject input) {
            return new Certificate(TranslatorUtils.toString(input, "class"),Countries.fromCode(TranslatorUtils.toString(input, "country")));
        }
    };

    @SuppressWarnings("unchecked")
    private void decodeContentGroups(DBObject dbObject, Content entity) {
        if (dbObject.containsField(CONTENT_GROUP_KEY)) {
            entity.setContentGroupRefs(Iterables.transform((Iterable<DBObject>) dbObject.get(CONTENT_GROUP_KEY), new Function<DBObject, ContentGroupRef>() {

                @Override
                public ContentGroupRef apply(DBObject input) {
                    return contentGroupRefTranslator.fromDBObject(input);
                }
            }));
        }
    }

    @SuppressWarnings("unchecked")
    private void decodeTopics(DBObject dbObject, Content entity) {
        if (dbObject.containsField(TOPICS_KEY)) {
            entity.setTopicRefs(Iterables.transform((Iterable<DBObject>) dbObject.get(TOPICS_KEY), new Function<DBObject, TopicRef>() {

                @Override
                public TopicRef apply(DBObject input) {
                    return contentTopicTranslator.fromDBObject(input);
                }
            }));
        }
    }

    private void decodeEvents(DBObject dbObject, Content entity) {
        if(dbObject.containsField(EVENTS_KEY)) {
            entity.setEventRefs(Iterables.transform((Iterable<DBObject>) dbObject.get(EVENTS_KEY), new Function<DBObject, EventRef>() {

                @Override
                public EventRef apply(DBObject dbObject) {
                    return eventRefTranslator.fromDBObject(dbObject);
                }
            }));
        }
    }

    @SuppressWarnings("unchecked")
    private void decodeKeyPhrases(DBObject dbObject, Content entity) {
        if (dbObject.containsField(PHRASES_KEY)) {
            entity.setKeyPhrases(Iterables.transform((Iterable<DBObject>) dbObject.get(PHRASES_KEY), new Function<DBObject, KeyPhrase>() {

                @Override
                public KeyPhrase apply(DBObject input) {
                    return keyPhraseTranslator.fromDBObject(input);
                }
            }));
        }
    }

    @SuppressWarnings("unchecked")
    private void decodeClips(DBObject dbObject, Content entity) {
        if (dbObject.containsField(CLIPS_KEY)) {
            Iterable<DBObject> clipsDbos = (Iterable<DBObject>) dbObject.get(CLIPS_KEY);
            List<Clip> clips = Lists.newArrayList();
            for (DBObject dbo : clipsDbos) {
                clips.add(clipTranslator.fromDBObject(dbo, null));
            }
            entity.setClips(clips);
        }

    }

    @Override
    public DBObject toDBObject(DBObject dbObject, Content entity) {
        dbObject = describedTranslator.toDBObject(dbObject, entity);
        encodeClips(dbObject, entity);
        encodeKeyPhrases(dbObject, entity);
        encodeTopics(dbObject, entity);
        encodeContentGroups(dbObject, entity);
        encodeLanguages(dbObject, entity);
        encodeEvents(dbObject, entity);
        encodeCertificates(dbObject, entity.getCertificates());
        TranslatorUtils.from(dbObject, YEAR_KEY, entity.getYear());
        TranslatorUtils.from(dbObject, GENERIC_DESCRIPTION_KEY, entity.getGenericDescription());
        TranslatorUtils.from(dbObject, EDITORIAL_PRIORITY_KEY, entity.getEditorialPriority());
        
        TranslatorUtils.from(dbObject, SIMILAR_CONTENT_KEY, similarContentRefTranslator.toDBList(entity.getSimilarContent()));
        if (! entity.people().isEmpty()) {
            BasicDBList list = new BasicDBList();
            for (CrewMember person: entity.people()) {
                list.add(crewMemberTranslator.toDBObject(null, person));
            }
            dbObject.put(PEOPLE, list);
        }
        
        if (!entity.getVersions().isEmpty()) {
            BasicDBList list = new BasicDBList();
            for (Version version: VERSION_ORDERING.immutableSortedCopy(entity.getVersions())) {
                if (version == null) {
                    throw new IllegalArgumentException("Cannot save item with null version: " + entity.getCanonicalUri());
                }
                list.add(versionTranslator.toDBObject(null, version));
            }
            dbObject.put(VERSIONS_KEY, list);
        }
        dbObject.put(AWARDS, awardTranslator.toDBList(entity.getAwards()));
        return dbObject;
    }


    protected void encodeLanguages(DBObject dbObject, Content entity) {
        if (!entity.getLanguages().isEmpty()) {
            TranslatorUtils.fromSet(dbObject, entity.getLanguages(), LANGUAGES_KEY);
        }
    }
    
    private void encodeCertificates(DBObject dbo, Set<Certificate> certificates) {
        if(!certificates.isEmpty()) {
            BasicDBList values = new BasicDBList();
            for(Certificate releaseDate : certificates) {
                DBObject certDbo = new BasicDBObject();
                TranslatorUtils.from(certDbo, "class", releaseDate.classification());
                TranslatorUtils.from(certDbo, "country", releaseDate.country().code());
                values.add(certDbo);
            }
            dbo.put(CERTIFICATES_KEY, values);
        }  
    }

    private void encodeContentGroups(DBObject dbObject, Content entity) {
        if (!entity.getContentGroupRefs().isEmpty()) {
            BasicDBList values = new BasicDBList();
            for(ContentGroupRef contentGroupRef : entity.getContentGroupRefs()) {
                values.add(contentGroupRefTranslator.toDBObject(contentGroupRef));
            }
            dbObject.put(CONTENT_GROUP_KEY, values);
        }
    }

    private void encodeTopics(DBObject dbObject, Content entity) {
        if (!entity.getTopicRefs().isEmpty()) {
            BasicDBList values = new BasicDBList();
            for(TopicRef topicRef : entity.getTopicRefs()) {
                values.add(contentTopicTranslator.toDBObject(topicRef));
            }
            dbObject.put(TOPICS_KEY, values);
        }
    }

    private void encodeEvents(DBObject dbObject, Content entity) {
        if(!entity.events().isEmpty()) {
            BasicDBList values = new BasicDBList();
            for(EventRef eventRef : entity.events()){
                values.add(eventRefTranslator.toDBObject(eventRef));
            }
            dbObject.put(EVENTS_KEY, values);
        }
    }

    private void encodeClips(DBObject dbObject, Content entity) {
        if (!entity.getClips().isEmpty()) {
            BasicDBList clipDbos = new BasicDBList();
            for (Clip clip : entity.getClips()) {
                clipDbos.add(clipTranslator.toDBObject(new BasicDBObject(), clip));
            }
            dbObject.put(CLIPS_KEY, clipDbos);
        }
    }

    private void encodeKeyPhrases(DBObject dbObject, Content entity) {
        if (!entity.getKeyPhrases().isEmpty()) {
            BasicDBList values = new BasicDBList();
            for(KeyPhrase phrase : entity.getKeyPhrases()) {
                values.add(keyPhraseTranslator.toDBObject(phrase));
            }
            dbObject.put(PHRASES_KEY, values);
        }
    }

    @SuppressWarnings("unchecked")
    public void removeFieldsForHash(DBObject dbObject) {
        describedTranslator.removeFieldsForHash(dbObject);
        Iterable<DBObject> clips = (Iterable<DBObject>) dbObject.get(ContentTranslator.CLIPS_KEY);
        if (clips != null) {
            Set<DBObject> unorderedClips = Sets.newHashSet();
            for (DBObject clipDbo : clips) {
                clipTranslator.removeFieldsForHash(clipDbo);
                unorderedClips.add(clipDbo);
            }
            dbObject.put(ContentTranslator.CLIPS_KEY, unorderedClips);
        }
        Iterable<DBObject> versions = (Iterable<DBObject>) dbObject.get(VERSIONS_KEY);
        if (versions != null) {
            dbObject.put(VERSIONS_KEY, removeUpdateTimeFromVersions(versions));
        }
    }

    private static final Ordering<Version> VERSION_ORDERING = new Ordering<Version>() {

        @Override
        public int compare(Version left, Version right) {
            return ComparisonChain.start()
                    .compare(left.getCanonicalUri(), right.getCanonicalUri(), Ordering.natural().nullsLast())
                    .compare(left.getCurie(), right.getCurie(), Ordering.natural().nullsLast())
                    .result();
        }

    };

    @SuppressWarnings("unchecked")
    private Set<DBObject> removeUpdateTimeFromVersions(Iterable<DBObject> versions) {
        Set<DBObject> unorderedVersions = Sets.newHashSet();
        for (DBObject versionDbo : versions) {
            versionDbo.removeField(IdentifiedTranslator.LAST_UPDATED);
            Iterable<DBObject> broadcasts = (Iterable<DBObject>) versionDbo.get(VersionTranslator.BROADCASTS_KEY);
            if (broadcasts != null) {
                Set<DBObject> unorderedBroadcasts = Sets.newHashSet();
                for (DBObject broadcastDbo : broadcasts) {
                    broadcastDbo.removeField(IdentifiedTranslator.LAST_UPDATED);
                    unorderedBroadcasts.add(broadcastDbo);
                }
                versionDbo.put(VersionTranslator.BROADCASTS_KEY, unorderedBroadcasts);
            }
            Iterable<DBObject> encodings = (Iterable<DBObject>) versionDbo.get(VersionTranslator.ENCODINGS_KEY);
            if (encodings != null) {
                versionDbo.put(VersionTranslator.ENCODINGS_KEY, removeUpdateTimesFromEncodings(encodings));
            }
            unorderedVersions.add(versionDbo);
        }
        return unorderedVersions;
    }

    @SuppressWarnings("unchecked")
    private Set<DBObject> removeUpdateTimesFromEncodings(Iterable<DBObject> encodings) {
        Set<DBObject> unorderedEncodings = Sets.newHashSet();
        for (DBObject encodingDbo : encodings) {
            encodingDbo.removeField(IdentifiedTranslator.LAST_UPDATED);
            Iterable<DBObject> locations = (Iterable<DBObject>) encodingDbo.get(EncodingTranslator.LOCATIONS_KEY);
            if (locations != null) {
                Set<DBObject> unorderedLocations = Sets.newHashSet();
                for (DBObject locationDbo : locations) {
                    locationDbo.removeField(IdentifiedTranslator.LAST_UPDATED);
                    DBObject policy = (DBObject) locationDbo.get(LocationTranslator.POLICY);
                    if(policy != null) {
                        policy.removeField(IdentifiedTranslator.LAST_UPDATED);
                    }
                    unorderedLocations.add(locationDbo);
                }
                encodingDbo.put(EncodingTranslator.LOCATIONS_KEY, unorderedLocations);
            }
            unorderedEncodings.add(encodingDbo);
        }
        return unorderedEncodings;
    }
}
