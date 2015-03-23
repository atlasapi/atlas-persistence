package org.atlasapi.persistence.media.entity;

import static org.atlasapi.persistence.events.EventTranslatorTest.createEvent;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.isNotNull;
import static org.mockito.Matchers.notNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;

import com.metabroadcast.common.persistence.mongo.MongoConstants;
import com.mongodb.DBObject;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Event;
import org.atlasapi.media.entity.EventRef;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Topic;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;


public class ContentTranslatorTest {

    private NumberToShortStringCodec idCodec = SubstitutionTableNumberCodec.lowerCaseOnly();
    private final ContentTranslator translator = new ContentTranslator(idCodec);
    
    @Test
    public void testEventTranslation() {
        Event event = createEvent(ImmutableList.<Topic>of());
        event.setId(1234l);
        List<Event> events = ImmutableList.of(event);
        Content content = createContentWithEvents(events);
        
        Content translated = translator.fromDBObject(translator.toDBObject(null, content), new Item());
        
        EventRef translatedEvent = Iterables.getOnlyElement(translated.events());
        
        assertEquals(event.getId(), translatedEvent.id());
    }
    
    @Test
    public void testEventRefTranslation() {
        EventRef event = new EventRef(1234l);
        List<EventRef> events = ImmutableList.of(event);
        Content content = createContentWithEventRefs(events);
        
        Content translated = translator.fromDBObject(translator.toDBObject(null, content), new Item());
        
        EventRef translatedEvent = Iterables.getOnlyElement(translated.events());
        
        assertEquals(event.id(), translatedEvent.id());
    }

    @Test
    public void testTranslateFromDboContentWithNullTermsOfUse() {
        DBObject dbObject = mock(DBObject.class);
        when(dbObject.get(MongoConstants.ID)).thenReturn("1");
        when(dbObject.get("description")).thenReturn("description");
        when(dbObject.get("termsOfUse")).thenReturn(null);
        Content content = translator.fromDBObject(dbObject, new Item());

        assertThat(content.getDescription(), is("description"));
    }

    @Test
    public void testTranslateFromDboContentWithTermsOfUse() {
        DBObject dbObject = mock(DBObject.class);
        DBObject termsOfUseDbo = mock(DBObject.class);
        when(termsOfUseDbo.containsField("text")).thenReturn(true);
        when(termsOfUseDbo.get("text")).thenReturn("ToU text");

        when(dbObject.get(MongoConstants.ID)).thenReturn("1");
        when(dbObject.get("description")).thenReturn("description");
        when(dbObject.containsField("termsOfUse")).thenReturn(true);
        when(dbObject.get("termsOfUse")).thenReturn(termsOfUseDbo);

        Content content = translator.fromDBObject(dbObject, new Item());

        assertThat(content.getDescription(), is("description"));
        assertThat(content.getTermsOfUse().getText(), is("ToU text"));
    }


    private Content createContentWithEventRefs(Iterable<EventRef> events) {
        Content content = new Item();
        content.setDescription("some content");
        content.setEventRefs(events);
        return content;
    }
    
    private Content createContentWithEvents(Iterable<Event> events) {
        Content content = new Item();
        content.setDescription("some content");
        content.setEvents(events);
        return content;
    }

}
