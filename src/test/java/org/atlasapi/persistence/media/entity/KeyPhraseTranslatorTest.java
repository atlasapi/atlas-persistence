package org.atlasapi.persistence.media.entity;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

import org.atlasapi.media.content.KeyPhrase;
import org.atlasapi.media.content.Publisher;
import org.junit.Test;

import com.mongodb.DBObject;

public class KeyPhraseTranslatorTest {

    private final KeyPhraseTranslator translator = new KeyPhraseTranslator();
    
    @Test
    public void encodesAndDecodesKeyPhrase() {

        KeyPhrase phrase = new KeyPhrase("a phrase", Publisher.BBC, 1.0);
        
        DBObject dbo = translator.toDBObject(phrase);
        
        KeyPhrase decoded = translator.fromDBObject(dbo);
        
        assertThat(decoded.getPhrase(), is(equalTo(phrase.getPhrase())));
        assertThat(phrase.getPublisher(), is(equalTo(phrase.getPublisher())));
        assertThat(decoded.getWeighting(), is(equalTo(phrase.getWeighting())));
    }

}
