package org.atlasapi.persistence.media.entity;

import org.atlasapi.media.entity.Author;

import com.metabroadcast.common.persistence.translator.TranslatorUtils;

import com.mongodb.DBObject;

public class AuthorTranslator {

    private static final String AUTHOR_NAME_KEY = "authorName";
    private static final String AUTHOR_INITIALS_KEY = "authorInitials";

    private AuthorTranslator() {

    }

    public static AuthorTranslator create() {
        return new AuthorTranslator();
    }

    public DBObject toDBObject(DBObject dbObject, Author model) {
        TranslatorUtils.from(dbObject, AUTHOR_NAME_KEY, model.getAuthorName());
        TranslatorUtils.from(dbObject, AUTHOR_INITIALS_KEY, model.getAuthorInitials());

        return dbObject;
    }

    public Author fromDBObject(DBObject dbObject) {
        if (TranslatorUtils.toString(dbObject, AUTHOR_NAME_KEY) == null &&
                TranslatorUtils.toString(dbObject, AUTHOR_INITIALS_KEY) == null) {
            return null;
        } else {
            return Author.builder()
                    .withAuthorName(TranslatorUtils.toString(dbObject, AUTHOR_NAME_KEY))
                    .withAuthorInitials(TranslatorUtils.toString(dbObject, AUTHOR_INITIALS_KEY))
                    .build();
        }
    }
}
