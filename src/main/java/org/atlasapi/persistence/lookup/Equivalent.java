package org.atlasapi.persistence.lookup;

import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.Publisher;

import com.google.common.base.Function;
import com.google.common.base.Objects;

public class Equivalent {

    private final String id;
    private final Publisher publisher;
    private final String type;

    public Equivalent(String id, Publisher publisher, String type) {
        this.id = id;
        this.publisher = publisher;
        this.type = type;
    }

    public String id() {
        return id;
    }

    public Publisher publisher() {
        return publisher;
    }

    public String type() {
        return type;
    }
    
    @Override
    public boolean equals(Object that) {
        if(this == that) {
            return true;
        }
        if(that instanceof Equivalent) {
            Equivalent other = (Equivalent) that;
            return id.equals(other.id) && publisher.equals(other.publisher) && type.equals(other.type);
        }
        return false;
    }
    
    @Override
    public int hashCode() {
        return Objects.hashCode(id, publisher, type);
    }
    
    @Override
    public String toString() {
        return String.format("%s(%s,%s)", id, publisher.title(), type);
    }

    public static Equivalent from(Described subject) {
        return new Equivalent(subject.getCanonicalUri(), subject.getPublisher(), subject.getType());
    }
    
    public static Function<Equivalent,String> TO_ID = new Function<Equivalent, String>() {
        @Override
        public String apply(Equivalent input) {
            return input.id();
        }
    };
}
