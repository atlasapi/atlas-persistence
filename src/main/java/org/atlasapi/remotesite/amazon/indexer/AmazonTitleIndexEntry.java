package org.atlasapi.remotesite.amazon.indexer;

import joptsimple.internal.Strings;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

//ENG-144
public class AmazonTitleIndexEntry {
    private final String title;
    private final Set<String> uris;
    private DateTime lastUpdated;

    public AmazonTitleIndexEntry(
            String title,
            @Nullable Collection<String> uris,
            @Nullable DateTime lastUpdated

    ) {
        checkArgument(!Strings.isNullOrEmpty(title));
        this.title = title;
        this.uris = uris != null ? new HashSet<>(uris) : new HashSet<>();
        this.lastUpdated = lastUpdated;
    }

    public AmazonTitleIndexEntry(
            String title,
            @Nullable Collection<String> uris
    ) {
        this(
                title,
                uris,
                null
        );
    }

    public AmazonTitleIndexEntry(
            String title
    ) {
        this(
                title,
                null,
                null
        );
    }

    public boolean addUri(String uri) {
        return uris.add(uri);
    }

    public void setLastUpdated(DateTime lastUpdated) {
        this.lastUpdated = lastUpdated;
    }

    public String getTitle() {
        return title;
    }

    public Set<String> getUris() {
        return new HashSet<>(uris);
    }

    @Nullable
    public DateTime getLastUpdated() {
        return lastUpdated;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AmazonTitleIndexEntry that = (AmazonTitleIndexEntry) o;
        return Objects.equals(title, that.title) &&
                Objects.equals(uris, that.uris);
    }

    @Override
    public int hashCode() {
        return Objects.hash(title, uris);
    }
}
