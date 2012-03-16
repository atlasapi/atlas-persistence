package org.atlasapi.persistence.media.entity;

import java.util.List;

import org.atlasapi.media.entity.Clip;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.KeyPhrase;
import org.atlasapi.media.entity.RelatedLink;
import org.atlasapi.media.entity.TopicRef;
import org.atlasapi.persistence.ModelTranslator;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
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
    private final ClipTranslator clipTranslator;
    private final KeyPhraseTranslator keyPhraseTranslator;
    private final DescribedTranslator describedTranslator;
    private final RelatedLinkTranslator relatedLinkTranslator;
    private final TopicRefTranslator contentTopicTranslator;
    private final ContentGroupRefTranslator contentGroupRefTranslator;

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
    }

    @Override
    public Content fromDBObject(DBObject dbObject, Content entity) {
        describedTranslator.fromDBObject(dbObject, entity);

        decodeClips(dbObject, entity);
        decodeKeyPhrases(dbObject, entity);
        decodeRelatedLinks(dbObject, entity);
        decodeTopics(dbObject, entity);
        decodeContentGroups(dbObject, entity);

        entity.setId(TranslatorUtils.toLong(dbObject, ID_KEY));

        return entity;
    }

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

        TranslatorUtils.from(dbObject, ID_KEY, entity.getId());

        return dbObject;
    }

    private void encodeContentGroups(DBObject dbObject, Content entity) {
        if (!entity.getContentGroupRefs().isEmpty()) {
            dbObject.put(CONTENT_GROUP_KEY, ImmutableSet.copyOf(Iterables.transform(entity.getContentGroupRefs(), new Function<ContentGroupRef, DBObject>() {

                @Override
                public DBObject apply(ContentGroupRef input) {
                    return contentGroupRefTranslator.toDBObject(input);
                }
            })));
        }
    }

    private void encodeTopics(DBObject dbObject, Content entity) {
        if (!entity.getTopicRefs().isEmpty()) {
            dbObject.put(TOPICS_KEY, ImmutableSet.copyOf(Iterables.transform(entity.getTopicRefs(), new Function<TopicRef, DBObject>() {

                @Override
                public DBObject apply(TopicRef input) {
                    return contentTopicTranslator.toDBObject(input);
                }
            })));
        }
    }

    private void encodeRelatedLinks(DBObject dbObject, Content entity) {
        if (!entity.getRelatedLinks().isEmpty()) {
            dbObject.put(LINKS_KEY, ImmutableSet.copyOf(Iterables.transform(entity.getRelatedLinks(), new Function<RelatedLink, DBObject>() {

                @Override
                public DBObject apply(RelatedLink input) {
                    return relatedLinkTranslator.toDBObject(input);
                }
            })));
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
            dbObject.put(PHRASES_KEY, ImmutableSet.copyOf(Iterables.transform(entity.getKeyPhrases(), new Function<KeyPhrase, DBObject>() {

                @Override
                public DBObject apply(KeyPhrase input) {
                    return keyPhraseTranslator.toDBObject(input);
                }
            })));
        }
    }
}
