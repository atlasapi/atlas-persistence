package org.atlasapi.persistence.content.mongo;

import static com.metabroadcast.common.persistence.mongo.MongoBuilders.where;
import static com.metabroadcast.common.persistence.mongo.MongoConstants.SINGLE;
import static com.metabroadcast.common.persistence.mongo.MongoConstants.UPSERT;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;

import java.util.Arrays;
import java.util.List;

import org.atlasapi.media.entity.EventRef;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.media.entity.IdentifiedTranslator;
import org.atlasapi.persistence.media.entity.ItemTranslator;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;

import com.google.common.collect.ImmutableList;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.persistence.MongoTestHelper;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.ReadPreference;


@RunWith( MockitoJUnitRunner.class )
public class MongoTopLevelItemsEntryTest {

    private static DatabasedMongo mongo;
    private static MongoTopLevelItemsEntry itemsEntry;
    private static DBCollection topLevelItems;
    private static Logger log = mock(Logger.class);
    private static ItemTranslator itemTranslator;

    @BeforeClass
    public static void setup() {
        mongo = MongoTestHelper.anEmptyTestDatabase();
        topLevelItems = mongo.collection("topLevelItems");
        itemsEntry = new MongoTopLevelItemsEntry(topLevelItems, ReadPreference.primary());
        itemTranslator = new ItemTranslator(new SubstitutionTableNumberCodec());
    }

    @After
    public void clear() {
        mongo.collection("topLevelItems").remove(new BasicDBObject());
        reset(log);
    }

    @Test
    public void getUrisForListOfEventIds() {

        Long eventIdOne = 123458L;
        EventRef eventRefOne = new EventRef(eventIdOne);
        eventRefOne.setPublisher(Publisher.BBC);

        String testOneUri = "http://test1.com/234";
        Item testItemOne = new Item();
        testItemOne.setCanonicalUri(testOneUri);
        testItemOne.setEventRefs(Arrays.asList(eventRefOne));

        String testTwoUri = "http://test2e.com/123";
        Item testItemTwo = new Item();
        testItemTwo.setCanonicalUri(testTwoUri);
        testItemTwo.setEventRefs(Arrays.asList(eventRefOne,eventRefOne));

        Long eventIdTwo = 123459L;
        EventRef eventRefTwo = new EventRef(eventIdTwo);
        eventRefTwo.setPublisher(Publisher.BBC);

        String testThreeUri = "http://test.com/123";
        Item testItemThree = new Item();
        testItemThree.setCanonicalUri(testThreeUri);
        testItemThree.setEventRefs(Arrays.asList(eventRefTwo));

        topLevelItems.update(where().fieldEquals(IdentifiedTranslator.ID,
                testItemOne.getCanonicalUri()).build(), itemTranslator.toDB(testItemOne), UPSERT, SINGLE);
        topLevelItems.update(where().fieldEquals(IdentifiedTranslator.ID,
                testItemTwo.getCanonicalUri()).build(), itemTranslator.toDB(testItemTwo), UPSERT, SINGLE);
        topLevelItems.update(where().fieldEquals(IdentifiedTranslator.ID,
                testItemThree.getCanonicalUri()).build(), itemTranslator.toDB(testItemThree), UPSERT, SINGLE);
        topLevelItems.createIndex(new BasicDBObject("events._id", 1));

        List<String> uris = ImmutableList.copyOf(itemsEntry.getItemUrisForEventIds(Arrays.asList(eventIdOne, eventIdTwo)));

        assertEquals(uris.get(0), testOneUri);
        assertEquals(uris.get(1), testTwoUri);
        assertEquals(uris.get(2), testThreeUri);

    }

}
