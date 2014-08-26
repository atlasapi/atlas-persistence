package org.atlasapi.persistence.event;

import org.atlasapi.media.entity.Event;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Topic;
import org.atlasapi.persistence.media.entity.ChildRefTranslator;
import org.atlasapi.persistence.media.entity.IdentifiedTranslator;
import org.atlasapi.persistence.media.entity.OrganisationTranslator;
import org.atlasapi.persistence.media.entity.PersonTranslator;
import org.atlasapi.persistence.media.entity.TopicTranslator;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;


public class EventTranslator {

    public static final String TITLE_KEY = "title";
    public static final String PUBLISHER_KEY = "publisher";
    public static final String VENUE_KEY = "venue";
    public static final String START_TIME_KEY = "startTime";
    public static final String END_TIME_KEY = "endTime";
    public static final String PARTICIPANTS_KEY = "participants";
    public static final String ORGANISATIONS_KEY = "organisations";
    public static final String EVENT_GROUPS_KEY = "eventGroups";
    public static final String CONTENT_KEY = "content";
    
    private final IdentifiedTranslator identifiedTranslator = new IdentifiedTranslator(true);
    private final TopicTranslator topicTranslator = new TopicTranslator();
    private final OrganisationTranslator organisationTranslator = new OrganisationTranslator();
    private final PersonTranslator personTranslator = new PersonTranslator();
    private final ChildRefTranslator childRefTranslator = new ChildRefTranslator();
    
    public DBObject toDBObject(Event event) {
        DBObject dbo = new BasicDBObject();
        
        identifiedTranslator.toDBObject(dbo, event);
        
        TranslatorUtils.from(dbo, TITLE_KEY, event.title());
        TranslatorUtils.from(dbo, PUBLISHER_KEY, event.publisher().key());
        TranslatorUtils.from(dbo, VENUE_KEY, topicTranslator.toDBObject(event.venue()));
        TranslatorUtils.fromDateTime(dbo, START_TIME_KEY, event.startTime());
        TranslatorUtils.fromDateTime(dbo, END_TIME_KEY, event.endTime());
        TranslatorUtils.fromIterable(dbo, PARTICIPANTS_KEY, event.participants(), personTranslator.translatePerson());
        TranslatorUtils.fromIterable(dbo, ORGANISATIONS_KEY, event.organisations(), organisationTranslator.translateToDBObject());
        TranslatorUtils.fromIterable(dbo, EVENT_GROUPS_KEY, event.eventGroups(), topicToDBObject());
        TranslatorUtils.from(dbo, CONTENT_KEY, childRefTranslator.toDBList(event.content()));
        
        return dbo;
    }

    public Event fromDBObject(DBObject dbo) {
        if (dbo == null) {
            return null;
        }
        
        Optional<Iterable<Topic>> eventGroups = TranslatorUtils.toIterable(dbo, EVENT_GROUPS_KEY, topicFromDBObject());
        
        @SuppressWarnings("deprecation") // Uses Maybe
        Event.Builder event = Event.builder()
                .withTitle(TranslatorUtils.toString(dbo, TITLE_KEY))
                .withPublisher(Publisher.fromKey(TranslatorUtils.toString(dbo, PUBLISHER_KEY)).requireValue())
                .withVenue(topicTranslator.fromDBObject(TranslatorUtils.toDBObject(dbo, VENUE_KEY)))
                .withStartTime(TranslatorUtils.toDateTime(dbo, START_TIME_KEY))
                .withEndTime(TranslatorUtils.toDateTime(dbo, END_TIME_KEY))
                .withParticipants(personTranslator.fromDBObjects(TranslatorUtils.toDBObjectList(dbo, PARTICIPANTS_KEY)))
                .withContent(childRefTranslator.fromDBObjects(TranslatorUtils.toDBObjectList(dbo, CONTENT_KEY)))
                .withOrganisations(organisationTranslator.fromDBObjects(TranslatorUtils.toDBObjectList(dbo, ORGANISATIONS_KEY)));
        
        if (eventGroups.isPresent()) {
            event = event.withEventGroups(eventGroups.get());
        }
        
        return (Event) identifiedTranslator.fromDBObject(dbo, event.build());
    }
    
    public Function<DBObject, Event> translateDBObject() {
        return new Function<DBObject, Event>() {
            @Override
            public Event apply(DBObject input) {
                return fromDBObject(input);
            }
        };
    }
    
    private Function<DBObject, Topic> topicFromDBObject() {
        return new Function<DBObject, Topic>() {
            @Override
            public Topic apply(DBObject input) {
                return topicTranslator.fromDBObject(input);
            }
        };
    }
    
    private Function<Topic, DBObject> topicToDBObject() {
        return new Function<Topic, DBObject>() {
            @Override
            public DBObject apply(Topic input) {
                return topicTranslator.toDBObject(input);
            }
        };
    }
}
