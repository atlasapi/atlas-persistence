package org.atlasapi.persistence.content.cassandra.json;

import com.google.common.collect.ImmutableList;
import com.metabroadcast.common.intl.Country;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Set;
import org.atlasapi.media.entity.Clip;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.CrewMember;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.KeyPhrase;
import org.atlasapi.media.entity.RelatedLink;
import org.atlasapi.media.entity.TopicRef;
import org.atlasapi.media.entity.Version;
import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonTypeInfo;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.codehaus.jackson.map.introspect.VisibilityChecker;
import org.codehaus.jackson.map.type.TypeFactory;

/**
 */
public class ContentMapper {

    private final ObjectMapper mapper;

    public ContentMapper() {
        this.mapper = new ObjectMapper();

        this.mapper.setSerializationInclusion(JsonSerialize.Inclusion.NON_NULL);

        this.mapper.setVisibilityChecker(VisibilityChecker.Std.defaultInstance().
                withSetterVisibility(JsonAutoDetect.Visibility.NONE).
                withGetterVisibility(JsonAutoDetect.Visibility.NONE).
                withIsGetterVisibility(JsonAutoDetect.Visibility.NONE).
                withCreatorVisibility(JsonAutoDetect.Visibility.ANY).
                withFieldVisibility(JsonAutoDetect.Visibility.ANY));

        this.mapper.getSerializationConfig().addMixInAnnotations(Content.class, ContentDescriptor.class);
        this.mapper.getDeserializationConfig().addMixInAnnotations(Content.class, ContentDescriptor.class);
        this.mapper.getSerializationConfig().addMixInAnnotations(Item.class, ItemDescriptor.class);
        this.mapper.getDeserializationConfig().addMixInAnnotations(Item.class, ItemDescriptor.class);
        this.mapper.getSerializationConfig().addMixInAnnotations(Country.class, CountryDescriptor.class);
        this.mapper.getDeserializationConfig().addMixInAnnotations(Country.class, CountryDescriptor.class);
        this.mapper.getSerializationConfig().addMixInAnnotations(CrewMember.class, CrewMemberDescriptor.class);
        this.mapper.getDeserializationConfig().addMixInAnnotations(CrewMember.class, CrewMemberDescriptor.class);
    }

    public <T extends Content> void serialize(T entity, ColumnWriter writer) throws Exception {
        String type = entity.getClass().getName();
        writer.write("type", type.getBytes(Charset.forName("UTF-8")));

        byte[] main = mapper.writeValueAsBytes(entity);
        writer.write("obj", main);

        byte[] clips = mapper.writeValueAsBytes(entity.getClips());
        writer.write("clips", clips);

        byte[] keyPhrases = mapper.writeValueAsBytes(entity.getKeyPhrases());
        writer.write("keyPhrases", keyPhrases);

        byte[] relatedLinks = mapper.writeValueAsBytes(entity.getRelatedLinks());
        writer.write("relatedLinks", relatedLinks);

        byte[] topicRefs = mapper.writeValueAsBytes(entity.getTopicRefs());
        writer.write("topicRefs", topicRefs);

        serializeItemSpecificFields(entity, writer);
    }

    public <T extends Content> T deserialize(ColumnReader reader) throws Exception {
        Class type = Class.forName(new String(reader.read("type"), Charset.forName("UTF-8")));

        T entity = (T) mapper.readValue(reader.read("obj"), type);

        List clips = mapper.readValue(reader.read("clips"), TypeFactory.defaultInstance().constructCollectionType(List.class, Clip.class));
        entity.setClips(clips);

        Set keyPhrases = mapper.readValue(reader.read("keyPhrases"), TypeFactory.defaultInstance().constructCollectionType(Set.class, KeyPhrase.class));
        entity.setKeyPhrases(keyPhrases);

        Set relatedLinks = mapper.readValue(reader.read("relatedLinks"), TypeFactory.defaultInstance().constructCollectionType(Set.class, RelatedLink.class));
        entity.setRelatedLinks(relatedLinks);

        List topicRefs = mapper.readValue(reader.read("topicRefs"), TypeFactory.defaultInstance().constructCollectionType(List.class, TopicRef.class));
        entity.setTopicRefs(topicRefs);

        deserializeItemSpecificFields(entity, reader);

        return entity;
    }

    private <T extends Content> void serializeItemSpecificFields(T value, ColumnWriter writer) throws Exception {
        if (value instanceof Item) {
            Item item = (Item) value;

            byte[] people = mapper.writerWithType(TypeFactory.defaultInstance().constructCollectionType(List.class, CrewMember.class)).writeValueAsBytes(item.getPeople());
            writer.write("people", people);

            byte[] versions = mapper.writeValueAsBytes(item.getVersions());
            writer.write("versions", versions);
        }
    }

    private <T extends Content> void deserializeItemSpecificFields(T entity, ColumnReader reader) throws Exception {
        if (entity instanceof Item) {
            Item item = (Item) entity;

            List people = mapper.readValue(reader.read("people"), TypeFactory.defaultInstance().constructCollectionType(List.class, CrewMember.class));
            item.setPeople(people);

            Set versions = mapper.readValue(reader.read("versions"), TypeFactory.defaultInstance().constructCollectionType(Set.class, Version.class));
            item.setVersions(versions);
        }
    }

    private static abstract class ContentDescriptor {

        @JsonIgnore
        ImmutableList<Clip> clips;
        @JsonIgnore
        Set<KeyPhrase> keyPhrases;
        @JsonIgnore
        Set<RelatedLink> relatedLinks;
        @JsonIgnore
        ImmutableList<TopicRef> topicRefs;
    }

    private static abstract class ItemDescriptor {

        @JsonIgnore
        Set<Version> versions;
        @JsonIgnore
        List<CrewMember> people;
    }

    private static abstract class CountryDescriptor {

        @JsonCreator
        CountryDescriptor(@JsonProperty(value = "code") String code, @JsonProperty(value = "name") String name) {
        }
    }

    @JsonTypeInfo(use=JsonTypeInfo.Id.CLASS, include=JsonTypeInfo.As.PROPERTY, property="@class")
    private static abstract class CrewMemberDescriptor {
    }
}
