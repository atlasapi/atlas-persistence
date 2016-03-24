package org.atlasapi.persistence.media.entity;

import static org.atlasapi.persistence.events.EventTranslatorTest.createEvent;
import static org.junit.Assert.assertEquals;

import java.util.List;

import org.atlasapi.media.entity.*;
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
        
        Content translated = translator.fromDBObject(translator.toDBObject(null, content), new Item(), true);
        
        EventRef translatedEvent = Iterables.getOnlyElement(translated.events());
        
        assertEquals(event.getId(), translatedEvent.id());
    }
    
    @Test
    public void testEventRefTranslation() {
        EventRef event = new EventRef(1234l, Publisher.BBC);
        List<EventRef> events = ImmutableList.of(event);
        Content content = createContentWithEventRefs(events);
        
        Content translated = translator.fromDBObject(translator.toDBObject(null, content), new Item(), true);
        
        EventRef translatedEvent = Iterables.getOnlyElement(translated.events());
        
        assertEquals(event.id(), translatedEvent.id());
        assertEquals(event.getPublisher(),translatedEvent.getPublisher());
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
