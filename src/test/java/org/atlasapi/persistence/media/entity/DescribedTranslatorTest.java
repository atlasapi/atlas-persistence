package org.atlasapi.persistence.media.entity;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.stub;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.*;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.LocalizedDescription;
import org.atlasapi.media.entity.LocalizedTitle;
import org.atlasapi.media.entity.Priority;
import org.atlasapi.media.entity.PriorityScoreReasons;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.RelatedLink;
import org.atlasapi.media.entity.Review;
import org.atlasapi.media.segment.Segment;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

@RunWith(MockitoJUnitRunner.class)
public class DescribedTranslatorTest {

    private final IdentifiedTranslator identifiedTranslator = mock(IdentifiedTranslator.class);
    
    @Test
    public void testHashCodeSymmetry() {
        
        stub(identifiedTranslator.toDBObject((DBObject)any(), (Described)any())).toAnswer(new Answer<DBObject>() {
            public DBObject answer(InvocationOnMock invocation) {
                return (DBObject) invocation.getArguments()[0];
            }
        });
        
        BasicDBList list = new BasicDBList();
        list.add(ImmutableMap.of("url", "http://example.com/", "type", "unknown"));
        list.add(ImmutableMap.of("url", "http://another.com/", "type", "unknown"));
        
        BasicDBList descriptionsList = new BasicDBList();
        descriptionsList.add(ImmutableMap.of("language", "en-GB", "shortDescription", "Desc 1", "description", "Desc 1"));
        descriptionsList.add(ImmutableMap.of("language", "en-US", "shortDescription", "Desc 2", "mediumDescription", "Desc 2 Medium", "description", "Desc 2 Medium"));

        BasicDBList localizedTitles = new BasicDBList();
        localizedTitles.add(ImmutableMap.of("title", "Title 5"));
        localizedTitles.add(ImmutableMap.of("locale", "en-GB", "title", "Title 1"));
        localizedTitles.add(ImmutableMap.of("locale", "it", "title", "Titolo 3"));
        localizedTitles.add(ImmutableMap.of("locale", "und", "title", "Title 4"));
        localizedTitles.add(ImmutableMap.of("locale", "und-US", "title", "Title 2"));

        Map<String, Object> m = ImmutableMap.of("links", (Object) list,
                "descriptions",
                (Object) descriptionsList,
                "titles",
                (Object) localizedTitles);
                
        DBObject dbo = new BasicDBObject(m);
        int hashCodeFromDbo = dbo.hashCode();
        
        Content content = new Item();
        
        content.setRelatedLinks(ImmutableSet.of(
                RelatedLink.unknownTypeLink("http://example.com/").build(),
                RelatedLink.unknownTypeLink("http://another.com/").build()
                ));
        
        content.setLocalizedDescriptions(localizedDescriptions());
        content.setLocalizedTitles(localizedTitles());
        
        DescribedTranslator translator = new DescribedTranslator(identifiedTranslator, null);
        
        BasicDBObject dboFromContent = new BasicDBObject();
        translator.toDBObject(dboFromContent, content);
        dboFromContent.remove(DescribedTranslator.MEDIA_TYPE_KEY);
        dboFromContent.remove(DescribedTranslator.IMAGES_KEY);
        dboFromContent.remove(DescribedTranslator.ACTIVELY_PUBLISHED_KEY);
        
        int hashCodeFromContent = dboFromContent.hashCode();
        
        System.out.println(dbo);
        System.out.println(dboFromContent);
        
        assertEquals(dbo, dboFromContent);
        assertEquals(hashCodeFromContent, hashCodeFromDbo);
    }
    
    @Test
    public void testLocalizedDescriptionsAndTitlesTranslation() {
        Content content = new Item();
        
        content.setLocalizedDescriptions(localizedDescriptions());
        content.setLocalizedTitles(localizedTitles());
        
        DescribedTranslator translator = new DescribedTranslator(identifiedTranslator, null);
        
        BasicDBObject dboFromContent = new BasicDBObject();
        translator.toDBObject(dboFromContent, content);       
        
        assertTrue(dboFromContent.containsField(DescribedTranslator.LOCALIZED_DESCRIPTIONS_KEY));
        assertTrue(dboFromContent.containsField(DescribedTranslator.LOCALIZED_TITLES_KEY));
        
        BasicDBList dboDescriptions = (BasicDBList) dboFromContent.get(DescribedTranslator.LOCALIZED_DESCRIPTIONS_KEY);
        assertEquals(localizedDescriptions().size(), dboDescriptions.size());
        
        BasicDBList dboTitles = (BasicDBList) dboFromContent.get(DescribedTranslator.LOCALIZED_TITLES_KEY);
        assertEquals(localizedTitles().size(), dboTitles.size());    
    }
    
    @Test
    public void testReviewsTranslation() {        
        Content content = new Item();
        DescribedTranslator translator = new DescribedTranslator(identifiedTranslator, null);
        
        content.setReviews(ImmutableSet.of(
                Review.builder()
                        .withLocale(Locale.ENGLISH)
                        .withReview("I am an English review.")
                        .withPublisherKey(Publisher.METABROADCAST.key())
                        .build(),
                Review.builder()
                        .withLocale(Locale.FRENCH)
                        .withReview("Je suis un examen en français.")
                        .withPublisherKey(Publisher.METABROADCAST.key())
                        .build()
        ));
        
        BasicDBObject dbo = new BasicDBObject();
        translator.toDBObject(dbo, content);
        
        assertEquals(2, ((BasicDBList)dbo.get(DescribedTranslator.REVIEWS_KEY)).size());
        
        Described fromDBO = translator.fromDBObject(dbo, new Item());
        
        assertEquals(content.getReviews(), fromDBO.getReviews());
        
    }

    @Test
    public void testPriorityTranslationFromDb() {
        Content content = new Item();
        DescribedTranslator translator = new DescribedTranslator(identifiedTranslator, null);

        content.setPriority(new Priority(new Double(47.0), new PriorityScoreReasons(
                ImmutableList.of("Positive reason test 1", "Positive reason test 2"),
                ImmutableList.of("Negative reason test 1", "Negative reason test 2", "Negative reason test 3")
        )));
        BasicDBObject dbo = new BasicDBObject();
        translator.toDBObject(dbo, content);

        Described fromDBO = translator.fromDBObject(dbo, new Item());

        assertEquals(content.getPriority().getScore(), fromDBO.getPriority().getScore());
        assertEquals(content.getPriority().getReasons().getPositive(),
                fromDBO.getPriority().getReasons().getPositive());
        assertEquals(content.getPriority().getReasons().getNegative(),
                fromDBO.getPriority().getReasons().getNegative());
    }

    @Test
    public void testPriorityTranslationToDb() {
        List<String> positiveScoreReasons = Lists.newArrayList();
        List<String> negativeScoreReasons = Lists.newArrayList();
        Content content = new Item();
        BasicDBObject dbo = new BasicDBObject();
        DescribedTranslator translator = new DescribedTranslator(identifiedTranslator, null);

        content.setPriority(new Priority(
                new Double(47.0),
                new PriorityScoreReasons(
                        ImmutableList.of("Positive reason test 1","Positive reason test 2","Positive reason test 3"),
                        ImmutableList.of("Negative reason test 1", "Negative reason test 2", "Negative reason test 3", "Negative reason test 4")
                )
        ));
        DBObject fromDBO = translator.toDBObject(dbo, content);

        DBObject priority = TranslatorUtils.toDBObject(fromDBO, "priority");
        Double score = TranslatorUtils.toDouble(priority, "score");
        DBObject reasons = TranslatorUtils.toDBObject(priority, "reasons");
        List<String> positiveReasons = TranslatorUtils.toList(reasons, "positive");
        List<String> negativeReasons = TranslatorUtils.toList(reasons, "negative");
        for (Object reason : positiveReasons) {
            if (reason != null && reason instanceof String) {
                String string = (String) reason;
                positiveScoreReasons.add(string);
            }
        }
        for (Object reason : negativeReasons) {
            if (reason != null && reason instanceof String) {
                String string = (String) reason;
                negativeScoreReasons.add(string);
            }
        }

        assertEquals(content.getPriority().getScore(), score);

        assertEquals(3, positiveReasons.size());
        assertEquals("Positive reason test 1", positiveReasons.get(0));
        assertEquals("Positive reason test 2", positiveReasons.get(1));
        assertEquals("Positive reason test 3", positiveReasons.get(2));

        assertEquals(4, negativeReasons.size());
        assertEquals("Negative reason test 1", negativeReasons.get(0));
        assertEquals("Negative reason test 2", negativeReasons.get(1));
        assertEquals("Negative reason test 3", negativeReasons.get(2));
        assertEquals("Negative reason test 4", negativeReasons.get(3));
    }

    @Test
    public void testTranslateFromDBObjectForSegmentWithObjectAsDescription() {
        Segment segment = new Segment();
        DBObject dboObject = new BasicDBObject();
        DBObject description = new BasicDBObject();
        description.put("title", "description title");
        dboObject.put("description", description);
        DescribedTranslator translator = new DescribedTranslator(identifiedTranslator, null);

        translator.fromDBObject(dboObject, segment);

        assertThat(segment.getDescription(), is("description title"));

    }

    @Test
    public void testTranslateFromDBObjectWithStringDescription() {
        Segment segment = new Segment();
        DBObject dboObject = new BasicDBObject();
        dboObject.put("description", "description title");
        DescribedTranslator translator = new DescribedTranslator(identifiedTranslator, null);

        translator.fromDBObject(dboObject, segment);

        assertThat(segment.getDescription(), is("description title"));

    }

    private Set<LocalizedDescription> localizedDescriptions() {
        Set<LocalizedDescription> localizedDescriptions = Sets.newHashSet();

        LocalizedDescription desc1 = new LocalizedDescription();
        desc1.setLocale(new Locale("en", "GB"));
        desc1.setDescription("Desc 1");
        desc1.setShortDescription("Desc 1");

        LocalizedDescription desc2 = new LocalizedDescription();
        desc2.setLocale(new Locale("en", "US"));
        desc2.setDescription("Desc 2 Medium");
        desc2.setShortDescription("Desc 2");
        desc2.setMediumDescription("Desc 2 Medium");

        localizedDescriptions.add(desc1);
        localizedDescriptions.add(desc2);

        return localizedDescriptions;
    }

    public static Set<LocalizedTitle> localizedTitles() {
        Set<LocalizedTitle> localizedTitles = Sets.newHashSet();

        LocalizedTitle titleWithLanguageAndRegion = new LocalizedTitle();   //i.e. title with locale
        titleWithLanguageAndRegion.setLocale(Locale.forLanguageTag("en-GB"));
        titleWithLanguageAndRegion.setTitle("Title 1");

        LocalizedTitle titleWithRegionOnly = new LocalizedTitle();
        titleWithRegionOnly.setLocale(new Locale("", "US"));
        titleWithRegionOnly.setTitle("Title 2");

        LocalizedTitle titleWithLanguageOnly = new LocalizedTitle();
        titleWithLanguageOnly.setLocale(new Locale("it"));
        titleWithLanguageOnly.setTitle("Titolo 3");

        LocalizedTitle titleWithEmptyLocale = new LocalizedTitle();
        titleWithEmptyLocale.setLocale(new Locale("",""));
        titleWithEmptyLocale.setTitle("Title 4");

        LocalizedTitle titleWithNullLocale = new LocalizedTitle();
        titleWithNullLocale.setTitle("Title 5");

        localizedTitles.add(titleWithLanguageAndRegion);
        localizedTitles.add(titleWithRegionOnly);
        localizedTitles.add(titleWithLanguageOnly);
        localizedTitles.add(titleWithEmptyLocale);
        localizedTitles.add(titleWithNullLocale);

        return localizedTitles;        
    }
    
}
