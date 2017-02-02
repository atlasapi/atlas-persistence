package org.atlasapi.persistence.media.entity;

import java.util.Locale;

import org.atlasapi.media.entity.Author;
import org.atlasapi.media.entity.Review;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ReviewTranslatorTest {

    private static final String LOCALE_KEY = "locale";
    private static final String REVIEW_KEY = "review";
    private static final String TYPE_KEY = "type";
    private static final String AUTHOR_NAME_KEY = "authorName";
    private static final String AUTHOR_INITIALS_KEY = "authorInitials";
    private ReviewTranslator reviewTranslator;

    public ReviewTranslatorTest() {
        reviewTranslator = new ReviewTranslator();
    }

    @Test
    public void translationOfReviewsToDatabaseObject() {

        Review review = makeReview();
        Author author = makeAuthor();
        review.setAuthor(author);

        DBObject dbo = new BasicDBObject();
        reviewTranslator.toDBObject(dbo, review);

        assertTrue(dbo.containsField(LOCALE_KEY));
        assertTrue(dbo.containsField(REVIEW_KEY));
        assertTrue(dbo.containsField(TYPE_KEY));


        assertEquals(review.getType(), dbo.get(TYPE_KEY));
        assertEquals(review.getReview(), dbo.get(REVIEW_KEY));

        // had a - instead of a _ without using 'toLanguageTag()' - which is why
        // .toLanguageTag() is used.
        assertEquals(review.getLocale().toLanguageTag(), dbo.get(LOCALE_KEY));

        assertTrue(dbo.containsField(AUTHOR_NAME_KEY));
        assertTrue(dbo.containsField(AUTHOR_INITIALS_KEY));

        assertEquals(author.getAuthorInitials(), dbo.get(AUTHOR_INITIALS_KEY));
        assertEquals(author.getAuthorName(), dbo.get(AUTHOR_NAME_KEY));

    }

    private Author makeAuthor() {
        return Author.builder()
                    .withAuthorInitials("A.M.")
                    .withAuthorName("Andy Miller")
                    .build();
    }

    private Review makeReview() {
        Review review = new Review(new Locale("en", "US"), "some string");
        review.setType("some type");
        return review;
    }

    @Test
    public void translationOfReviewsFromDatabaseObject() {
        Review review = makeReview();
        Author author = makeAuthor();
        review.setAuthor(author);

        DBObject dbo = new BasicDBObject();
        reviewTranslator.toDBObject(dbo, review);

        Review output = reviewTranslator.fromDBObject(dbo);

        assertEquals(review.getLocale(), output.getLocale());
        assertEquals(review.getType(), output.getType());
        assertEquals(review.getReview(), output.getReview());

        Author authorOutput = output.getAuthor();

        assertEquals(author.getAuthorName(), authorOutput.getAuthorName());
        assertEquals(author.getAuthorInitials(), authorOutput.getAuthorInitials());
    }
}