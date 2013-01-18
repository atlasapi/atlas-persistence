package org.atlasapi.persistence.media.entity;

import java.util.List;
import java.util.Set;

import org.atlasapi.media.common.Id;
import org.atlasapi.media.content.Content;
import org.atlasapi.media.entity.Certificate;
import org.atlasapi.media.entity.Clip;
import org.atlasapi.media.entity.CrewMember;
import org.atlasapi.media.entity.KeyPhrase;
import org.atlasapi.media.entity.RelatedLink;
import org.atlasapi.media.entity.TopicRef;
import org.atlasapi.persistence.media.ModelTranslator;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.intl.Countries;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.atlasapi.media.entity.ContentGroupRef;

public class ContentTranslator implements ModelTranslator<Content> {

    public static String CLIPS_KEY = "clips";
    public static String TOPICS_KEY = "topics";
    private static String ID_KEY = "aid";
    private static final String LINKS_KEY = "links";
    private static final String PHRASES_KEY = "phrases";
    private static String CONTENT_GROUP_KEY = "contentGroups";
    private static final String CERTIFICATES_KEY = "certificates";
    private static final String LANGUAGES_KEY = "languages";
    private static final String YEAR_KEY = "year";
    private final ClipTranslator clipTranslator;
    private final KeyPhraseTranslator keyPhraseTranslator;
    private final DescribedTranslator describedTranslator;
    private final RelatedLinkTranslator relatedLinkTranslator;
    private final TopicRefTranslator contentTopicTranslator;
    private final ContentGroupRefTranslator contentGroupRefTranslator;
    private final CrewMemberTranslator crewMemberTranslator;

    public ContentTranslator(NumberToShortStringCodec idCodec) {
        this(new DescribedTranslator(new IdentifiedTranslator()), new ClipTranslator(idCodec));
    }

    //TODO: why not use collaborators interface here? ModelTranslator<Described> etc...
    public ContentTranslator(DescribedTranslator describedTranslator, ClipTranslator clipTranslator) {
        this.describedTranslator = describedTranslator;
        this.clipTranslator = clipTranslator;
        this.keyPhraseTranslator = new KeyPhraseTranslator();
        this.relatedLinkTranslator = new RelatedLinkTranslator();
        this.contentTopicTranslator = new TopicRefTranslator();
        this.contentGroupRefTranslator = new ContentGroupRefTranslator();
        this.crewMemberTranslator = new CrewMemberTranslator();
    }

    @Override
    public Content fromDBObject(DBObject dbObject, Content entity) {
        describedTranslator.fromDBObject(dbObject, entity);

        decodeClips(dbObject, entity);
        decodeKeyPhrases(dbObject, entity);
        decodeRelatedLinks(dbObject, entity);
        decodeTopics(dbObject, entity);
        decodeContentGroups(dbObject, entity);
        decodeLanguages(dbObject, entity);
        decodeCertificates(dbObject, entity); 
        entity.setYear(TranslatorUtils.toInteger(dbObject, YEAR_KEY));
        
        try {
            entity.setId(Id.valueOf(TranslatorUtils.toLong(dbObject, ID_KEY)));
        } catch (ClassCastException e) {
            entity.setId(Id.valueOf(TranslatorUtils.toDouble(dbObject, ID_KEY).longValue()));
        }
        
        List<DBObject> list = TranslatorUtils.toDBObjectList(dbObject, "people");
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

    @SuppressWarnings("unchecked")
    private void decodeRelatedLinks(DBObject dbObject, Content entity) {
        if (dbObject.containsField(LINKS_KEY)) {
            entity.setRelatedLinks(Iterables.transform((Iterable<DBObject>) dbObject.get(LINKS_KEY), new Function<DBObject, RelatedLink>() {

                @Override
                public RelatedLink apply(DBObject input) {
                    return relatedLinkTranslator.fromDBObject(input);
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
        encodeRelatedLinks(dbObject, entity);
        encodeTopics(dbObject, entity);
        encodeContentGroups(dbObject, entity);
        encodeLanguages(dbObject, entity);
        encodeCertificates(dbObject, entity.getCertificates());
        TranslatorUtils.from(dbObject, YEAR_KEY, entity.getYear());
        
        if (! entity.people().isEmpty()) {
            BasicDBList list = new BasicDBList();
            for (CrewMember person: entity.people()) {
                list.add(crewMemberTranslator.toDBObject(null, person));
            }
            dbObject.put("people", list);
        }

        TranslatorUtils.from(dbObject, ID_KEY, entity.getId());

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

    private void encodeRelatedLinks(DBObject dbObject, Content entity) {
        if (!entity.getRelatedLinks().isEmpty()) {
            BasicDBList values = new BasicDBList(); 
            for(RelatedLink link : entity.getRelatedLinks()) {
                values.add(relatedLinkTranslator.toDBObject(link));
            }
            dbObject.put(LINKS_KEY, values);
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
    
    public DBObject removeFieldsForHash(DBObject dbObject) {
        if (dbObject == null) {
            return null;
        }
        dbObject.removeField(DescribedTranslator.LAST_FETCHED_KEY);
        dbObject.removeField(DescribedTranslator.THIS_OR_CHILD_LAST_UPDATED_KEY);
        dbObject.removeField(IdentifiedTranslator.LAST_UPDATED);
        @SuppressWarnings("unchecked")
        Iterable<DBObject> clips = (Iterable<DBObject>) dbObject.get(CLIPS_KEY);
        if (clips != null) {
            Set<DBObject> unorderedClips = Sets.newHashSet();
            for (DBObject clipDbo : clips) {
                unorderedClips.add(clipTranslator.removeFieldsForHash(clipDbo));
            }
            dbObject.put(CLIPS_KEY, unorderedClips);
        }
        return dbObject;
    }
}
