package org.atlasapi.persistence.content.cassandra.json;

import java.io.IOException;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Content.Clips;
import org.atlasapi.media.entity.Content.KeyPhrases;
import org.atlasapi.media.entity.Content.RelatedLinks;
import org.atlasapi.media.entity.Content.TopicRefs;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.Version;
import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializerProvider;
import org.codehaus.jackson.map.introspect.VisibilityChecker;
import org.codehaus.jackson.map.module.SimpleModule;
import org.codehaus.jackson.map.ser.std.SerializerBase;

/**
 */
public class ContentMapper {

    private final ObjectMapper contentMapper;
    private final ObjectMapper childrenMapper;

    public ContentMapper() {
        this.contentMapper = new ObjectMapper();
        this.childrenMapper = new ObjectMapper();
        
        this.contentMapper.setVisibilityChecker(VisibilityChecker.Std.defaultInstance().
                withCreatorVisibility(JsonAutoDetect.Visibility.ANY).
                withFieldVisibility(JsonAutoDetect.Visibility.ANY));
        this.childrenMapper.setVisibilityChecker(VisibilityChecker.Std.defaultInstance().
                withCreatorVisibility(JsonAutoDetect.Visibility.ANY).
                withFieldVisibility(JsonAutoDetect.Visibility.ANY));
        
        SimpleModule module = new SimpleModule("Atlas", Version.unknownVersion());
        module.addSerializer(new IgnoringSerializer(Content.Clips.class));
        module.addSerializer(new IgnoringSerializer(Content.KeyPhrases.class));
        module.addSerializer(new IgnoringSerializer(Content.RelatedLinks.class));
        module.addSerializer(new IgnoringSerializer(Content.TopicRefs.class));
        contentMapper.registerModule(module);
    }

    public <T extends Content> void serialize(T value, ColumnWriter writer) throws Exception {
        String type = value.getClass().getName();
        writer.write("type", type);
        
        String main = contentMapper.writeValueAsString(value);
        writer.write("obj", main);
        
        String clips = childrenMapper.writeValueAsString(new Clips(value.getClips()));
        writer.write("clips", clips);
        
        String keyPhrases = childrenMapper.writeValueAsString(new KeyPhrases(value.getKeyPhrases()));
        writer.write("keyPhrases", keyPhrases);
        
        String relatedLinks = childrenMapper.writeValueAsString(new RelatedLinks(value.getRelatedLinks()));
        writer.write("relatedLinks", relatedLinks);
        
        String topicRefs = childrenMapper.writeValueAsString(new TopicRefs(value.getTopicRefs()));
        writer.write("topicRefs", topicRefs);
    }
    
    public <T extends Content> T deserialize(ColumnReader reader) throws Exception {
        Class type = Class.forName(reader.read("type"));
        
        T entity = (T) contentMapper.readValue(reader.read("obj"), type);
        
        Content.Clips clips = childrenMapper.readValue(reader.read("clips"), Clips.class);
        entity.setClips(clips.values);
        
        Content.KeyPhrases keyPhrases = childrenMapper.readValue(reader.read("keyPhrases"), KeyPhrases.class);
        entity.setClips(clips.values);
        
        RelatedLinks relatedLinks = childrenMapper.readValue(reader.read("relatedLinks"), RelatedLinks.class);
        entity.setRelatedLinks(relatedLinks.values);
        
        TopicRefs topicRefs = childrenMapper.readValue(reader.read("topicRefs"), TopicRefs.class);
        entity.setTopicRefs(topicRefs.values);
        
        return entity;
    }
    
    protected static class IgnoringSerializer extends SerializerBase {

        public IgnoringSerializer(Class clazz) {
            super(clazz);
        }
        
        @Override
        public void serialize(Object value, JsonGenerator generator, SerializerProvider provider) throws IOException, JsonGenerationException {
        }
    }
}
